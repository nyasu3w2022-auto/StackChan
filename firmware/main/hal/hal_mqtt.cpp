/*
 * SPDX-FileCopyrightText: 2026 M5Stack Technology CO LTD
 *
 * SPDX-License-Identifier: MIT
 */

/**
 * @file hal_mqtt.cpp
 * @brief MQTT service for machine ON/OFF control (publish) and text-to-alert
 *        (subscribe) via MCP tools.
 *
 * Publish — machine power control
 * --------------------------------
 * MCP tool "self.machine.set_power" enqueues a publish request via
 * mqttPublishMachine().  The service tick dequeues and publishes to:
 *   <kTopicPrefix>/<machine_name>   payload "1" (ON) or "0" (OFF)
 *
 * Subscribe — text alert
 * ----------------------
 * After connecting, the service subscribes to kSpeakTopic.  When a message
 * arrives the payload is forwarded to the Xiaozhi main task via
 * Application::Schedule() (hal_bridge::app_schedule) so that:
 *   1. Application::Alert() updates the on-screen display.
 *   2. TimedSpeechModifier + SpeakingModifier animate the avatar mouth.
 *   3. app_play_sound() plays a notification chime.
 *
 * Thread-safety
 * -------------
 * OnMessage() is called from the esp-mqtt internal task.  All UI/display
 * operations are dispatched to the Xiaozhi main task via app_schedule() to
 * avoid holding any LVGL lock from the mqtt task context, eliminating the
 * risk of priority-inversion deadlock between the mqtt_task and lvgl_port_task.
 *
 * Implementation note — why FreeRTOS task instead of BasicAbility
 * ---------------------------------------------------------------
 * startMqttMachineService() is called from Hal::startXiaozhi(), which runs
 * AFTER DestroyMooncake() in main.cpp.  At that point the Mooncake singleton
 * has been destroyed and recreated as an empty instance with no update() loop
 * driving it.  Registering a BasicAbility via extensionManager()->createAbility()
 * would therefore never have its onRunning() called.
 *
 * The same pattern used by _stackchan_update_task (xTaskCreatePinnedToCore)
 * is used here so the update loop runs independently of Mooncake.
 *
 * Startup sequence
 * ----------------
 * startMqttMachineService() is called before Application::Initialize() ->
 * board.StartNetwork(), so WiFi and the lwIP TCP/IP stack are not yet up.
 * To avoid the "Invalid mbox" assert, update() checks
 * WifiManager::IsConnected() and skips _connect() while WiFi is down.
 * Once WiFi comes up the reconnect timer fires immediately.
 *
 * Configuration placeholders
 * --------------------------
 * Replace the values in MqttMachineConfig with your actual broker settings
 * before building.  A future improvement could load these from NVS via the
 * Settings class (see hal_account.cpp for an example).
 *
 *   kBrokerHost   : hostname or IP address of the MQTT broker
 *   kBrokerPort   : broker port (1883 for plain TCP, 8883 for TLS)
 *   kClientId     : client identifier sent during CONNECT
 *   kTopicPrefix  : topic prefix for publish; full topic is
 *                   "<prefix>/<machine_name>"
 *   kSpeakTopic   : topic to subscribe for text-to-alert messages
 *
 * Publish payload
 * ---------------
 * When a machine is turned ON  the payload "1" is published.
 * When a machine is turned OFF the payload "0" is published.
 * QoS 0 is used by default; change kPublishQos if needed.
 */

#include "hal.h"
#include "board/hal_bridge.h"
#include <assets/assets.h>
#include <assets.h>
#include <mooncake_log.h>
#include <board.h>
#include <display.h>
#include <mqtt.h>
#include <wifi_manager.h>
#include <stackchan/stackchan.h>
#include <stackchan/modifiers/timed.h>
#include <lvgl_font.h>
#include <lvgl_theme.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <mutex>
#include <queue>
#include <string>
#include <memory>

/* -------------------------------------------------------------------------- */
/*                     JapaneseSpeechModifier                                  */
/* -------------------------------------------------------------------------- */

/**
 * @brief A TimedSpeechModifier that temporarily switches the speech bubble font
 *        to font_noto_qwen_20_4 (which contains Japanese glyphs) for the
 *        duration of the animation, then restores the theme default font.
 *
 * Font lifecycle:
 *   _on_start() : load font_noto_qwen_20_4.bin via Assets, call setSpeechTextFont()
 *   _on_end()   : restore the theme's default text font via Display::GetTheme()
 *
 * The LvglCBinFont object is owned by this modifier and is destroyed when the
 * modifier itself is destroyed (i.e., after _on_end() fires).
 */
class JapaneseSpeechModifier : public stackchan::TimedSpeechModifier {
public:
    JapaneseSpeechModifier(std::string_view speech, uint32_t durationMs)
        : stackchan::TimedSpeechModifier(speech, durationMs)
    {
    }

    void _on_start(stackchan::Modifiable& sc) override
    {
        // Load the Japanese-capable font from the Assets partition.
        void*  font_data = nullptr;
        size_t font_size = 0;
        if (Assets::GetInstance().GetAssetData(
                "font_noto_qwen_20_4.bin", font_data, font_size) && font_data) {
            _jp_font = std::make_unique<LvglCBinFont>(font_data);
            sc.avatar().setSpeechTextFont((void*)_jp_font->font());
        }
        // Let the base class set the speech text.
        stackchan::TimedSpeechModifier::_on_start(sc);
    }

    void _on_end(stackchan::Modifiable& sc) override
    {
        // Let the base class clear the speech text first.
        stackchan::TimedSpeechModifier::_on_end(sc);
        // Restore the theme's default font.
        auto* display = Board::GetInstance().GetDisplay();
        if (display) {
            auto* theme = static_cast<LvglTheme*>(display->GetTheme());
            if (theme && theme->text_font()) {
                sc.avatar().setSpeechTextFont((void*)theme->text_font()->font());
            }
        }
        // Release the Japanese font object.
        _jp_font.reset();
    }

private:
    std::unique_ptr<LvglCBinFont> _jp_font;
};

/* -------------------------------------------------------------------------- */
/*                            Configuration                                    */
/* -------------------------------------------------------------------------- */

namespace {

struct MqttMachineConfig {
    // TODO: Replace with your actual broker address
    static constexpr const char* kBrokerHost   = "mqtt.example.com";
    static constexpr int         kBrokerPort   = 1883;
    // TODO: Replace with a meaningful client identifier
    static constexpr const char* kClientId     = "stackchan-machine-ctrl";
    // TODO: Replace with your topic prefix (no trailing slash)
    static constexpr const char* kTopicPrefix  = "stackchan/machine";
    // Topic to subscribe for text-to-alert messages
    // Publish a UTF-8 string payload to this topic and StackChan will display
    // it on screen with a mouth-flap animation.
    static constexpr const char* kSpeakTopic   = "stackchan/speak";
    static constexpr int         kPublishQos   = 0;
    static constexpr int         kSubscribeQos = 0;
    // Reconnect interval in milliseconds
    static constexpr uint32_t    kReconnectMs  = 5000;
    // Update tick interval in milliseconds
    static constexpr uint32_t    kTickMs       = 100;
    // Duration (ms) for the mouth-flap animation when a speak message arrives
    static constexpr uint32_t    kSpeakAnimMs  = 4000;
};

}  // namespace

/* -------------------------------------------------------------------------- */
/*                          MqttMachineService class                           */
/* -------------------------------------------------------------------------- */

static const std::string _tag = "HAL-MQTT";

/**
 * @brief Encapsulates the MQTT connection, publish logic, and subscribe
 *        text-to-alert logic.
 *
 * Driven by a dedicated FreeRTOS task (_mqtt_machine_task) so that it
 * continues to run after DestroyMooncake() is called in main.cpp.
 *
 * Thread-safety for publishMachine() is provided by an internal
 * mutex-guarded queue so that MCP tool callbacks (running in the Xiaozhi
 * audio task) can safely enqueue publish requests without blocking.
 *
 * OnMessage() callbacks are dispatched to the Xiaozhi main task via
 * hal_bridge::app_schedule() to keep all LVGL/display operations off the
 * esp-mqtt internal task, avoiding potential deadlocks.
 */
class MqttMachineService {
public:
    struct PublishRequest {
        std::string machineName;
        bool        powerOn;
    };

    void update()
    {
        // Guard: never touch the TCP/IP stack while WiFi is not connected.
        if (!WifiManager::GetInstance().IsConnected()) {
            // Reset the reconnect timer so that _connect() fires immediately
            // after WiFi comes up rather than waiting kReconnectMs.
            _last_reconnect_attempt = 0;
            return;
        }

        if (!_mqtt || !_mqtt->IsConnected()) {
            if (GetHAL().millis() - _last_reconnect_attempt > MqttMachineConfig::kReconnectMs) {
                mclog::tagInfo(_tag, "reconnecting...");
                _connect();
            }
        } else {
            _processPendingRequests();
        }
    }

    /**
     * @brief Enqueue a power-control publish request (thread-safe).
     *
     * Safe to call from any task, including the MCP tool callback that runs
     * in the Xiaozhi audio task context.
     *
     * @param machineName  Logical name of the target machine.
     * @param powerOn      true = turn ON (publish "1"), false = turn OFF (publish "0").
     */
    void publishMachine(const std::string& machineName, bool powerOn)
    {
        std::lock_guard<std::mutex> lock(_queue_mutex);
        _publish_queue.push({machineName, powerOn});
        mclog::tagInfo(_tag, "enqueued: machine={}, power={}", machineName, powerOn ? "ON" : "OFF");
    }

    bool isConnected() const
    {
        return _mqtt && _mqtt->IsConnected();
    }

private:
    std::unique_ptr<Mqtt> _mqtt;
    uint32_t              _last_reconnect_attempt = 0;

    std::mutex                    _queue_mutex;
    std::queue<PublishRequest>    _publish_queue;

    void _connect()
    {
        // Destroy any stale instance before creating a new one.
        _mqtt.reset();

        auto& board   = Board::GetInstance();
        auto  network = board.GetNetwork();

        _mqtt = network->CreateMqtt(2);  // connect_id=2 (0: protocol, 1: ezdata)
        if (!_mqtt) {
            mclog::tagError(_tag, "failed to create MQTT instance");
            _last_reconnect_attempt = GetHAL().millis();
            return;
        }

        _mqtt->OnConnected([this]() {
            mclog::tagInfo(_tag, "connected to broker {}:{}", MqttMachineConfig::kBrokerHost,
                           MqttMachineConfig::kBrokerPort);
            // Subscribe to the speak topic once connected.
            if (_mqtt->Subscribe(MqttMachineConfig::kSpeakTopic, MqttMachineConfig::kSubscribeQos)) {
                mclog::tagInfo(_tag, "subscribed to {}", MqttMachineConfig::kSpeakTopic);
            } else {
                mclog::tagError(_tag, "failed to subscribe to {}", MqttMachineConfig::kSpeakTopic);
            }
        });

        _mqtt->OnDisconnected([this]() {
            mclog::tagInfo(_tag, "disconnected from broker");
        });

        _mqtt->OnError([this](const std::string& err) {
            mclog::tagError(_tag, "MQTT error: {}", err);
        });

        /**
         * OnMessage callback — runs in the esp-mqtt internal task context.
         *
         * All UI operations are dispatched to the Xiaozhi main task via
         * app_schedule() to avoid any LVGL lock contention.
         */
        _mqtt->OnMessage([](const std::string& topic, const std::string& payload) {
            mclog::tagInfo(_tag, "received: topic={}, payload={}", topic, payload);

            if (topic == MqttMachineConfig::kSpeakTopic) {
                // Capture payload by value so it is valid when the lambda runs
                // in the Xiaozhi main task.
                std::string text = payload;

                hal_bridge::app_schedule([text]() {
                    auto& sc = GetStackChan();
                    if (!sc.hasAvatar()) return;

                    // 1. Play a notification chime.
                    hal_bridge::app_play_sound(OGG_NEW_NOTIFICATION);

                    // 2. Animate the avatar mouth and display the speech bubble.
                    //    JapaneseSpeechModifier switches to font_noto_qwen_20_4 on
                    //    start and restores the theme font on end, so the LvglCBinFont
                    //    object lifetime is tied to the modifier's lifetime.
                    sc.addModifier(
                        std::make_unique<JapaneseSpeechModifier>(text, MqttMachineConfig::kSpeakAnimMs));
                    sc.addModifier(
                        std::make_unique<stackchan::SpeakingModifier>(MqttMachineConfig::kSpeakAnimMs));
                });
            }
        });

        mclog::tagInfo(_tag, "connecting to {}:{} as {}", MqttMachineConfig::kBrokerHost,
                       MqttMachineConfig::kBrokerPort, MqttMachineConfig::kClientId);

        // No authentication — pass empty strings for username and password.
        _mqtt->Connect(MqttMachineConfig::kBrokerHost, MqttMachineConfig::kBrokerPort,
                       MqttMachineConfig::kClientId, "", "");

        _last_reconnect_attempt = GetHAL().millis();
    }

    void _processPendingRequests()
    {
        std::vector<PublishRequest> requests;
        {
            std::lock_guard<std::mutex> lock(_queue_mutex);
            while (!_publish_queue.empty()) {
                requests.push_back(std::move(_publish_queue.front()));
                _publish_queue.pop();
            }
        }

        for (const auto& req : requests) {
            std::string topic   = fmt::format("{}/{}", MqttMachineConfig::kTopicPrefix, req.machineName);
            std::string payload = req.powerOn ? "1" : "0";

            bool ok = _mqtt->Publish(topic, payload, MqttMachineConfig::kPublishQos);
            if (ok) {
                mclog::tagInfo(_tag, "published: topic={}, payload={}", topic, payload);
            } else {
                mclog::tagError(_tag, "publish failed: topic={}", topic);
            }
        }
    }
};

/* -------------------------------------------------------------------------- */
/*                         FreeRTOS task                                       */
/* -------------------------------------------------------------------------- */

static MqttMachineService* _service_ptr = nullptr;

static void _mqtt_machine_task(void* param)
{
    auto* service = static_cast<MqttMachineService*>(param);
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(MqttMachineConfig::kTickMs));
        service->update();
    }
}

/* -------------------------------------------------------------------------- */
/*                          Hal public API                                     */
/* -------------------------------------------------------------------------- */

static std::unique_ptr<MqttMachineService> _service;

void Hal::startMqttMachineService(std::function<void(std::string_view)> onStartLog)
{
    mclog::tagInfo(_tag, "start mqtt machine service");

    if (onStartLog) {
        onStartLog("Starting MQTT\nmachine service...");
    }

    _service     = std::make_unique<MqttMachineService>();
    _service_ptr = _service.get();

    // Spawn a dedicated FreeRTOS task on core 1 (same as _stackchan_update_task).
    // Stack size 4096 bytes is sufficient for the MQTT client operations.
    xTaskCreatePinnedToCore(_mqtt_machine_task, "mqtt_machine", 4096, _service_ptr, 3, NULL, 1);
}

void Hal::mqttPublishMachine(const std::string& machineName, bool powerOn)
{
    if (!_service_ptr) {
        mclog::tagError(_tag, "mqttPublishMachine: service not started");
        return;
    }
    _service_ptr->publishMachine(machineName, powerOn);
}

void Hal::mqttSubscribeAlert()
{
    // Subscribe is set up automatically in _connect() -> OnConnected callback.
    // This method exists as a named entry point for documentation and future
    // per-topic configuration (e.g., loading kSpeakTopic from NVS).
    mclog::tagInfo(_tag, "mqttSubscribeAlert: speak topic={}", MqttMachineConfig::kSpeakTopic);
}
