package edu.ubb.dissertation.sensordatahandler.repository;

import edu.ubb.dissertation.sensordatahandler.exception.ConnectionException;
import edu.ubb.dissertation.sensordatahandler.model.SensorData;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.*;
import java.util.Arrays;

@Repository
public class SensorDataRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(SensorDataRepository.class);

    private static final String CONNECTION_URL = "jdbc:hive2://192.168.56.103:10000/dissertation";
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private Connection connection;
    private Statement statement;

    @PostConstruct
    public void postConstruct() {
        Try.run(() -> Class.forName(DRIVER_NAME))
                .onFailure(t -> LOGGER.error("Could not get driver. Message: {}", t.getMessage()))
                .getOrElseThrow(ConnectionException::create);
        connection = Try.of(() -> DriverManager.getConnection(CONNECTION_URL))
                .onFailure(t -> LOGGER.error("Could not create connection. Message: {}", t.getMessage()))
                .getOrElseThrow(ConnectionException::create);
        statement = Try.of(() -> connection.createStatement())
                .onFailure(t -> LOGGER.error("Could not create Statement. Message: {}", t.getMessage()))
                .getOrElseThrow(ConnectionException::create);
    }

    public void save(final SensorData sensorData) {
        final String columnNames = "timestamp, arm_id, force, temperature, pressure, rotation";
        final String query = String.format("INSERT INTO sensor_data(%s) VALUES('%s', '%s', %.2f, %.2f, %.2f, %.2f)",
                columnNames, Timestamp.valueOf(sensorData.getTimestamp()), sensorData.getArmId(), sensorData.getForce(),
                sensorData.getTemperature(), sensorData.getPressure(), sensorData.getRotation());
        Try.run(() -> statement.execute(query))
                .onFailure(t -> LOGGER.error("Could not insert entry for timestamp {}. Message: {}",
                        sensorData.getTimestamp(), t.getMessage()));
    }

    @PreDestroy
    public void preDestroy() {
        Try.run(() -> {
            statement.close();
            connection.close();
        }).onFailure(t -> LOGGER.error("Could not close connections. StackTrace: {}", Arrays.asList(t.getStackTrace())));
    }
}
