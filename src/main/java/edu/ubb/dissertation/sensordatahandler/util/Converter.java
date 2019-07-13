package edu.ubb.dissertation.sensordatahandler.util;

import io.vavr.control.Try;
import org.apache.commons.math3.util.Precision;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static java.time.ZoneOffset.UTC;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.math.NumberUtils.DOUBLE_ZERO;
import static org.apache.commons.lang3.math.NumberUtils.LONG_ZERO;

public final class Converter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

    private Converter() {
    }

    public static String convertByteArrayToString(final byte[] message, final Charset encoding) {
        return Try.of(() -> new String(message, encoding))
                .getOrElseGet(e -> new String(message, Charset.defaultCharset()));
    }

    public static Optional<JSONObject> convertByteArrayToJson(final byte[] message, final Charset encoding) {
        return convertStringToJson(convertByteArrayToString(message, encoding));
    }

    private static Optional<JSONObject> convertStringToJson(final String message) {
        return Try.of(() -> new JSONObject(message))
                .onFailure(t -> LOGGER.error("Could not convertByteArrayToString string to json. Message: {}", t.getMessage()))
                .map(Optional::of)
                .getOrElseGet(t -> Optional.empty());
    }

    public static Optional<LocalDateTime> extractTimestampFromEpoch(final JSONObject json, final String key) {
        final Long epochMillis = extractLong(json, key);
        return epochMillis == 0
                ? Optional.empty()
                : Optional.of(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochMillis), ZoneId.from(UTC)));
    }

    public static Double extractDoubleWithTwoDecimals(final JSONObject json, final String key) {
        return Try.of(() -> roundValue(json.getDouble(key)))
                .onFailure(t -> LOGGER.error("JSON does not contain double value for key: {}. Message: {}", key, t.getMessage()))
                .getOrElseGet(e -> DOUBLE_ZERO);
    }

    public static String extractString(final JSONObject json, final String key) {
        return Try.of(() -> json.getString(key))
                .onFailure(t -> LOGGER.error("JSON does not contain string value for key: {}. Message: {}", key, t.getMessage()))
                .getOrElseGet(e -> EMPTY);
    }

    private static Long extractLong(final JSONObject json, final String key) {
        return Try.of(() -> json.getLong(key))
                .onFailure(t -> LOGGER.error("JSON does not contain long value for key: {}. Message: {}", key, t.getMessage()))
                .getOrElseGet(e -> LONG_ZERO);
    }

    private static Double roundValue(final double value) {
        return Precision.round(value, 2);
    }
}
