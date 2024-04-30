package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * e92283e54e6e6e930ff8500841a68a6c057f865c - Serial using streams, lines -> forEach. CHM -> forEach took ~135 seconds.
 * be07811561db261f029f42deb33a0c612f3a4b76 - Multithreaded processing v1 with serial preprocessing v0.1. Output mechanism incomplete. Dies on memory usage.
 * */
public class CalculateAverage_ShannonChristie {
    public static boolean readerHasFinished = false;

    public static void main(String[] args) {
        Instant start = Instant.now();

        /////////////////////
        /// Configuration ///
        /////////////////////
        final int BATCH_SIZE = 100_000;

        //////////////////////////
        /// Auto-configuration ///
        //////////////////////////
        Runtime runtime = Runtime.getRuntime();
        int cores = runtime.availableProcessors();

        LinkedBlockingQueue queue = new LinkedBlockingQueue<ArrayList<String>>(cores + 2);

        Thread readerThread = new Thread(() -> {
            try (BufferedReader reader = Files.newBufferedReader(Path.of("./measurements.txt"))) {
                int currentIndex = 0; // Maintain "progress" of read

                Stream<String> linesStream = reader.lines();

                while (!readerHasFinished) {
                    final int offset = BATCH_SIZE * currentIndex++;

                    System.out.printf("Reader: about to start at %d", offset);

                    ArrayList<String> collect = linesStream
                            .skip(offset) // Progress through the stream
                            .limit(BATCH_SIZE)
                            .collect(Collectors.toCollection(ArrayList::new));

                    System.out.printf("Reader: completed read at %d for %d lines", offset, collect.size());

                    // If workers can't complete a batch in 20 seconds when we start to block
                    // something must've gone wrong.
                    queue.offer(collect, 20, TimeUnit.SECONDS);

                    if (collect.size() < BATCH_SIZE) { // We've clearly reached the end of the file
                        readerHasFinished = true;
                    }
                }
            } catch (IOException ex) {
                System.err.println("Reader: error reading file");

                System.err.println(ex.getMessage());
            } catch (InterruptedException ex) {
                System.err.println("Reader: workers couldn't process fast enough, we timed out at 20 seconds");

                System.err.println(ex.getMessage());
            } finally {
                readerHasFinished = true;
            }
        });

        readerThread.start();

        System.out.println("Reader thread started");



        CountDownLatch threadProcessingCompletionLatch = new CountDownLatch(cores);

        ArrayList<ArrayList<ProcessedLine>> processedLines =
                new ArrayList<>(cores);

        for (int i = 0; i < cores; i++) {
            processedLines.add(new ArrayList<>(BATCH_SIZE));

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
