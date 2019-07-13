package edu.ubb.dissertation.sensordatahandler.mqtt;

import edu.ubb.dissertation.sensordatahandler.model.SensorData;
import edu.ubb.dissertation.sensordatahandler.repository.SensorDataRepository;
import io.vavr.control.Try;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

import static edu.ubb.dissertation.sensordatahandler.util.Converter.*;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class SensorMqttCallback implements MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SensorMqttCallback.class);

    @Autowired
    private SensorDataRepository sensorDataRepository;

    @Override
    public void connectionLost(final Throwable cause) {
        // no need for reconnect attempt since the client was setup to automatically reconnect
        LOGGER.error(format("MQTT Broker connection was lost. Exception message: %s.", cause.getMessage()));
    }

    @Override
    public void messageArrived(final String topic, final MqttMessage mqttMessage) throws Exception {
        convertByteArrayToJson(mqttMessage.getPayload(), UTF_8)
                .flatMap(this::create)
                .ifPresent(sensorData -> sensorDataRepository.save(sensorData));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        Try.of(token::getMessage)
                .onSuccess(message -> LOGGER.info(format("Message %s consumed.", convertByteArrayToString(message.getPayload(), UTF_8))))
                .onFailure(e -> LOGGER.error(format("Failed to deliver message. Exception: %s", e.getMessage())));
    }

    private Optional<SensorData> create(final JSONObject json) {
        return extractTimestampFromEpoch(json, "timestamp")
                // needed since when the sensor starts it usually emits an inaccurate timestamp value
                .filter(timestamp -> timestamp.getYear() >= 2019)
                .map(timestamp -> create(json, timestamp));
    }

    private SensorData create(final JSONObject json, final LocalDateTime timestamp) {
        return new SensorData.Builder()
                .withTimestamp(timestamp)
                .withArmId(extractString(json, "armId"))
                .withRotation(extractDoubleWithTwoDecimals(json, "rotation"))
                .withTemperature(extractDoubleWithTwoDecimals(json, "temperature"))
                .withForce(extractDoubleWithTwoDecimals(json, "force"))
                .withPressure(extractDoubleWithTwoDecimals(json, "pressure"))
                .build();
    }
}
