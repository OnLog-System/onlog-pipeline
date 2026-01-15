package onlog.streams.parser;

import java.util.Base64;

public class BatteryPayloadDecoder {

    public static Decoded decode(String base64) {

        byte[] data = Base64.getDecoder().decode(base64);

        // =========================
        // Length validation
        // =========================
        if (data.length < 6) {
            throw new IllegalArgumentException(
                "payload too short: " + data.length
            );
        }

        // =========================
        // BAT (2 bytes)
        // =========================
        int batRaw = ((data[0] & 0xFF) << 8) | (data[1] & 0xFF);

        int statusBits = (batRaw >> 14) & 0b11;
        int voltageMv  = batRaw & 0x3FFF;

        String status = switch (statusBits) {
            case 0b00 -> "ULTRA_LOW";
            case 0b01 -> "LOW";
            case 0b10 -> "OK";
            case 0b11 -> "GOOD";
            default -> "UNKNOWN";
        };

        // =========================
        // Temperature (int16 / 100)
        // =========================
        short tempRaw = (short) (
                ((data[2] & 0xFF) << 8) |
                (data[3] & 0xFF)
        );
        double temperature = tempRaw / 100.0;

        // =========================
        // Humidity (uint16 / 10)
        // =========================
        int humRaw = ((data[4] & 0xFF) << 8) | (data[5] & 0xFF);
        double humidity = humRaw / 10.0;

        return new Decoded(voltageMv, status, temperature, humidity);
    }

    public record Decoded(
            int batteryMv,
            String batteryStatus,
            double temperature,
            double humidity
    ) {}
}
