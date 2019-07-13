package edu.ubb.dissertation.sensordatahandler.model;

import java.time.LocalDateTime;

public class SensorData {

    private LocalDateTime timestamp;
    private String armId;
    private Double rotation;
    private Double temperature;
    private Double force;
    private Double pressure;

    public SensorData() {
    }

    private SensorData(final Builder builder) {
        this.timestamp = builder.timestamp;
        this.armId = builder.armId;
        this.rotation = builder.rotation;
        this.temperature = builder.temperature;
        this.force = builder.force;
        this.pressure = builder.pressure;
    }

    public static class Builder {

        private LocalDateTime timestamp;
        private String armId;
        private Double rotation;
        private Double temperature;
        private Double force;
        private Double pressure;

        public Builder withTimestamp(final LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withArmId(final String armId) {
            this.armId = armId;
            return this;
        }

        public Builder withRotation(final Double rotation) {
            this.rotation = rotation;
            return this;
        }

        public Builder withTemperature(final Double temperature) {
            this.temperature = temperature;
            return this;
        }

        public Builder withForce(final Double force) {
            this.force = force;
            return this;
        }

        public Builder withPressure(final Double pressure) {
            this.pressure = pressure;
            return this;
        }

        public SensorData build() {
            return new SensorData(this);
        }
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getArmId() {
        return armId;
    }

    public Double getRotation() {
        return rotation;
    }

    public Double getTemperature() {
        return temperature;
    }

    public Double getForce() {
        return force;
    }

    public Double getPressure() {
        return pressure;
    }

}
