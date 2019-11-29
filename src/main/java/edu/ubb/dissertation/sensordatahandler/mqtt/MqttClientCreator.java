package edu.ubb.dissertation.sensordatahandler.mqtt;

import edu.ubb.dissertation.sensordatahandler.exception.ConnectionException;
import io.vavr.CheckedRunnable;
import io.vavr.control.Try;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MqttClientCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttClientCreator.class);

    private static final String CLIENT_ID = "SENSOR_CLIENT_ID";
    private static final String TOPIC_NAME = "dissertation/sensor_data_1";
    private static final String BROKER_ADDRESS = "tcp://mqtt.eclipse.org:1883";

    @Autowired
    private SensorMqttCallback sensorMqttCallback;

    @PostConstruct
    public void postConstruct() {
        initializeMqttClient();
    }

    private void initializeMqttClient() {
        Try.of(() -> new MqttClient(BROKER_ADDRESS, CLIENT_ID))
                .onSuccess(this::configureClient)
                .onFailure(e -> LOGGER.error("An error occurred while initializing the MQTT client. Message: %s", e.getMessage()));
    }

    private void configureClient(final MqttClient client) {
        execute(() -> client.connect(createConnectionOptions()),
                "Successfully connected to MQTT server", "Failed to connect to MQTT server, {}");
        client.setCallback(sensorMqttCallback);
        execute(() -> client.subscribe(TOPIC_NAME),
                "Successfully subscribed to MQTT server", "Failed to subscribe to topic, {}");
    }

    private void execute(final CheckedRunnable runnable, final String successMessage, final String failureMessage) {
        Try.run(runnable)
                .onSuccess(v -> LOGGER.info(successMessage))
                .onFailure(t -> LOGGER.error(failureMessage, t.getMessage()))
                .getOrElseThrow(ConnectionException::create);
    }

    private MqttConnectOptions createConnectionOptions() {
        final MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(false);
        // since the data from the sensor will be generated at an interval of 10 seconds,
        // this interval should ensure that the connection to the server does not timeout
        options.setKeepAliveInterval(1200);
        return options;
    }
}
