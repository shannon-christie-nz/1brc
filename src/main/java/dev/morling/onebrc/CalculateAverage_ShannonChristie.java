package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * e92283e54e6e6e930ff8500841a68a6c057f865c - Serial using streams, lines -> forEach. CHM -> forEach took ~135 seconds.
 * */
public class CalculateAverage_ShannonChristie {
    public static void main(String[] args) {
        Instant start = Instant.now();

        ArrayList<LineToProcess> lineToProcess = readLinesToBeProcessed();

        System.out.printf(
                "\nPreprocessed the data in %.4f",
                (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0
        );

        Runtime runtime = Runtime.getRuntime();
        int cores = runtime.availableProcessors();

        final int BATCH_SIZE = 100_000;
        final int BATCHES_PER_CORE = (lineToProcess.size() / BATCH_SIZE) / cores;

        final int ITEMS_PER_CORE = BATCH_SIZE * BATCHES_PER_CORE;

        System.out.printf(
                "\nGot %d cores with %d batches per core",
                cores,
                BATCHES_PER_CORE
        );

        CountDownLatch threadProcessingCompletionLatch = new CountDownLatch(cores);

        ArrayList<ArrayList<ProcessedLine>> processedLines =
                new ArrayList<>(cores);

        for (int i = 0; i < cores; i++) {
            processedLines.add(new ArrayList<>(ITEMS_PER_CORE));

            final int THREAD_INDEX = i;
            final int OFFSET = i * BATCH_SIZE;

            Thread t = new Thread(() -> {
                ArrayList<ProcessedLine> processedLinesPerThread = processedLines.get(THREAD_INDEX);

                int batchIndex = 0;

                while (batchIndex < BATCHES_PER_CORE) {
                    System.out.printf(
                            "\n%d - Starting batch %d",
                            THREAD_INDEX,
                            batchIndex
                    );

                    int startIndex = batchIndex * BATCH_SIZE + OFFSET;
                    int endIndex = Math.min(startIndex + BATCH_SIZE, lineToProcess.size());

                    for (int j = startIndex; j < endIndex; j++) {
                        LineToProcess item = lineToProcess.get(j);

                        ProcessedLine processedLine = new ProcessedLine(
                                item.line().substring(0, item.delimiterIndex()),
                                Double.parseDouble(item.line().substring(item.delimiterIndex()))
                        );

                        processedLinesPerThread.add(processedLine);
                    }

                    batchIndex++;
                }

                System.out.printf(
                        "\n%d - Completed all batches",
                        THREAD_INDEX
                );

                threadProcessingCompletionLatch.countDown();
            });

            t.start();
        }

        System.out.printf("\nAll threads spawned. %d threads", cores);

        try {
            if (!threadProcessingCompletionLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Timed out waiting for thread processing completion.");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for thread processing completion");

            throw new RuntimeException(e);
        }

        System.out.println("\nProcessing the data");

        ConcurrentHashMap<String, StationReport> reports = new ConcurrentHashMap<>(10_000);

        reports.forEach((stationName, report) -> {
            System.out.printf("%s=%.2f/%.2f/%.2f", stationName, report.getMin(), (report.getMax() - report.getMin()) / 2, report.getMax());
        });

        System.out.printf("\nTook %.4f", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
    }

    private static ArrayList<LineToProcess> readLinesToBeProcessed() {
        ArrayList<LineToProcess> lineToProcesses = new ArrayList<>(1_000_000_000);

        try (BufferedReader bufferedReader = Files.newBufferedReader(
                Path.of("./measurements.txt"),
                StandardCharsets.UTF_8)) {

            bufferedReader
                    .lines()
                    .forEach(line -> lineToProcesses.add(new LineToProcess(line.indexOf(';'), line)));

        } catch (IOException ex) {
            System.err.printf("\nSomething went horribly wrong. Err: %s", ex.getMessage());

            throw new RuntimeException(ex);
        }

        return lineToProcesses;
    }

    public static record LineToProcess(int delimiterIndex, String line) {}

    public static record ProcessedLine(String stationName, double temperature) {}

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
