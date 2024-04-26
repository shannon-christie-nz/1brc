package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * e92283e54e6e6e930ff8500841a68a6c057f865c - Serial using streams, lines -> forEach. CHM -> forEach took ~135 seconds.
 * */
public class CalculateAverage_ShannonChristie {
    public static void main(String[] args) {
        Instant start = Instant.now();

        ConcurrentHashMap<String, StationReport> reports = new ConcurrentHashMap<>(10000);

        try (BufferedReader bufferedReader = Files.newBufferedReader(Path.of("./measurements.txt"), StandardCharsets.UTF_8)) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        for (int i = 0; i < line.length(); i++) {
                            if (line.charAt(i) == ';') {
                                String stationName = line.substring(0, i);
                                StationReport report = reports.getOrDefault(stationName, new StationReport(stationName));

                                String temperature = line.substring(i + 1);

                                double temp = Double.parseDouble(temperature);
                                report.setMin(Math.min(report.getMin(), temp));
                                report.setMax(Math.max(report.getMax(), temp));

                                reports.put(stationName, report);
                            }
                        }
                    });
        } catch (IOException ex) {
            System.err.printf("\nSomething went horribly wrong. Err: %s", ex.getMessage());
        }

        System.out.println("\nProcessing the data");

        reports.forEach((stationName, report) -> {
            System.out.printf("%s=%.2f/%.2f/%.2f", stationName, report.getMin(), (report.getMax() - report.getMin()) / 2, report.getMax());
        });

        System.out.printf("\nTook %.4f", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
    }

    public static class StationReport {
        private final String stationName;
        private double min;
        private double max;

        public StationReport(String stationName) {
            this.stationName = stationName;
        }

        public String getStationName() {
            return stationName;
        }

        public void setMax(double max) {
            this.max = max;
        }

        public double getMax() {
            return max;
        }

        public void setMin(double min) {
            this.min = min;
        }

        public double getMin() {
            return min;
        }
    }
}
