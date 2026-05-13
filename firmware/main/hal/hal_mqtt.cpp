/*
 * SPDX-FileCopyrightText: 2026 M5Stack Technology CO LTD
 *
 * SPDX-License-Identifier: MIT
 */

/**
 * @file hal_mqtt.cpp
 * @brief MQTT service for machine ON/OFF control via MCP tools.
 *
 * This module manages a single persistent MQTT connection used to publish
 * power-control commands to external machines.
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
 *   kTopicPrefix  : topic prefix; the full topic will be
 *                   "<prefix>/<machine_name>"
 *
 * Publish payload
 * ---------------
 * When a machine is turned ON  the payload "1" is published.
 * When a machine is turned OFF the payload "0" is published.
 * QoS 0 is used by default; change kPublishQos if needed.
 */

#include "hal.h"
#include <mooncake_log.h>
#include <board.h>
#include <mqtt.h>
#include <wifi_manager.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <mutex>
#include <queue>
#include <string>
#include <memory>

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
    static constexpr int         kPublishQos   = 0;
    // Reconnect interval in milliseconds
    static constexpr uint32_t    kReconnectMs  = 5000;
    // Update tick interval in milliseconds
    static constexpr uint32_t    kTickMs       = 100;
};

}  // namespace

/* -------------------------------------------------------------------------- */
/*                          MqttMachineService class                           */
/* -------------------------------------------------------------------------- */

static const std::string _tag = "HAL-MQTT";

/**
 * @brief Encapsulates the MQTT connection and publish logic.
 *
 * Driven by a dedicated FreeRTOS task (_mqtt_machine_task) so that it
 * continues to run after DestroyMooncake() is called in main.cpp.
 * Thread-safety for publishMachine() is provided by an internal
 * mutex-guarded queue so that MCP tool callbacks (running in the Xiaozhi
 * audio task) can safely enqueue publish requests without blocking.
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
        });

        _mqtt->OnDisconnected([this]() {
            mclog::tagInfo(_tag, "disconnected from broker");
        });

        _mqtt->OnError([this](const std::string& err) {
            mclog::tagError(_tag, "MQTT error: {}", err);
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
