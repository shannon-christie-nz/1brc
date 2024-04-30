package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * e92283e54e6e6e930ff8500841a68a6c057f865c - Serial using streams, lines -> forEach. CHM -> forEach took ~135 seconds.
 * be07811561db261f029f42deb33a0c612f3a4b76 - Multithreaded processing v1 with serial preprocessing v0.1. Output mechanism incomplete. Dies on memory usage.
 * 2b7a6e7b9fe6dce7fab14bc7ced015aa0a544248 - Multithreaded processing v2 (thread per core) with serial reader thread. Using blocking work queue. Output complete. No memory death. Took ~56 seconds
 * cea6548e9ffc2a3b3771ac374ba13c673387d1e1 - Multithreaded, 1 reader 1 worker. Blocking queue. Took ~52 seconds.
 * */
public class CalculateAverage_ShannonChristie {
    public static boolean readerHasFinished = false;

    public static void main(String[] args) {
        Instant start = Instant.now();

        /////////////////////
        /// Configuration ///
        /////////////////////
        final int BATCH_SIZE = 100_000_000;

        //////////////////////////
        /// Auto-configuration ///
        //////////////////////////
//        Runtime runtime = Runtime.getRuntime();
//        int cores = runtime.availableProcessors();
        int cores = 1;

        LinkedBlockingQueue<ArrayList<String>> queue = new LinkedBlockingQueue<>(cores + 2);

        Thread readerThread = new Thread(() -> {
            try (RandomAccessFile reader = new RandomAccessFile("./measurements.txt", "r")) {
                long currentIndex = 0; // Maintain "progress" of read

                FileChannel channel = reader.getChannel();

                while (!readerHasFinished) {
                    Instant readerStart = Instant.now();

                    System.out.printf("Reader: about to start at %d\n", currentIndex);

                    // Map new section of file
                    MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentIndex, BATCH_SIZE);

                    // Track last successfully read cursor position
                    long lastReadIndex = mappedByteBuffer.limit() + currentIndex;

                    // Ensure it's in memory
                    mappedByteBuffer.load();

                    // Start the walk finding lines
                    int currentReadLimit = mappedByteBuffer.limit();
                    int lastLineIndex = 0;
                    ArrayList<String> lines = new ArrayList<>(BATCH_SIZE);
                    for (int i = 0; i < currentReadLimit; i++) {
                        if (mappedByteBuffer.get(i) == '\n') {
                            CharBuffer lineBuffer = StandardCharsets.ISO_8859_1.decode(mappedByteBuffer.slice(lastLineIndex, i - lastLineIndex));
                            lastLineIndex = i + 1;

                            String newLine = lineBuffer.toString();

                            lines.add(newLine);
                        }
                    }

                    // In case the last character wasn't a new line
                    // when we hit the end of the memory section
                    if (lastLineIndex != currentReadLimit) {
                        // It may mean we have an incomplete line, we'll
                        // start again from this point
                        lastReadIndex = currentIndex + lastLineIndex;
                    }

                    System.out.printf("Reader: completed read at %d for %d lines\n", currentIndex, lines.size());

                    // If workers can't complete a batch in 20 seconds when we start to block
                    // something must've gone wrong.
                    queue.offer(lines, 20, TimeUnit.SECONDS);

                    if (currentIndex == lastReadIndex) { // We've clearly reached the end of the file
                        readerHasFinished = true;
                    }

                    currentIndex = lastReadIndex;

                    mappedByteBuffer.clear();

                    System.out.printf("Reader: read %d lines in %.2f seconds\n", lines.size(), (Instant.now().toEpochMilli() - readerStart.toEpochMilli()) / 1000.0);
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

        ArrayList<ConcurrentHashMap<String, StationReport>> inProgressReports = new ArrayList<>(cores);

        for (int i = 0; i < cores; i++) {
            inProgressReports.add(new ConcurrentHashMap<>());

            final int THREAD_INDEX = i;

            Thread t = new Thread(() -> {
                ConcurrentHashMap<String, StationReport> threadSpecificReport = inProgressReports.get(THREAD_INDEX);

                try {
                    while (true) {
                        // If this takes more than 4 seconds, we either finished or something went wrong
                        ArrayList<String> list = queue.poll(4, TimeUnit.SECONDS);

                        if (list == null) {
                            System.out.println("Thread " + THREAD_INDEX + ": no more data");

                            if (!readerHasFinished) {
                                System.err.println("Thread " + THREAD_INDEX + ": reader hadn't finished");

                                throw new RuntimeException("Thread " + THREAD_INDEX + ": no more data yet reader hadn't finished");
                            }

                            break;
                        }

                        System.out.println("Thread " + THREAD_INDEX + ": got work item");

                        Instant workerStart = Instant.now();

                        list
                                .stream()
                                .forEach((String line) -> {
                                    try {
                                        int delimiterIndex = line.indexOf(";");
                                        String stationName = line.substring(0, delimiterIndex);
                                        double temperature = Double.parseDouble(line.substring(delimiterIndex + 1));

                                        StationReport report = threadSpecificReport.get(stationName);
                                        if (report == null) {
                                            report = new StationReport(stationName);
                                            threadSpecificReport.put(stationName, report);
                                        }

                                        report.setSum(report.getSum() + temperature);
                                        report.setCount(report.getCount() + 1);
                                        report.setMax(Math.max(report.getMax(), temperature));
                                        report.setMin(Math.min(report.getMin(), temperature));
                                    } catch (NumberFormatException e) {
                                        System.err.printf("Error parsing temperature in line: %s\n", line);
                                    }
                                });

                        System.out.printf("Worker %d: completed work item in %.2f seconds\n", THREAD_INDEX, (Instant.now().toEpochMilli() - workerStart.toEpochMilli()) / 1000.0);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    // We completed all of our data, or something else went wrong
                    threadProcessingCompletionLatch.countDown();
                }
            });

            t.start();
        }

        System.out.printf("All threads spawned. %d threads\n", cores);



        try {
            if (!threadProcessingCompletionLatch.await(180, TimeUnit.SECONDS)) {
                throw new RuntimeException("Timed out waiting for thread processing completion.");
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for thread processing completion");

            throw new RuntimeException(e);
        }



        System.out.println("Processing the data");

        ConcurrentHashMap<String, StationReport> reports = new ConcurrentHashMap<>(10_000);

        inProgressReports.forEach((threadMap) -> {
            System.out.println("Got thread map, processing");

            threadMap.forEach((ignored, report) -> {
                StationReport station = reports.get(report.stationName);

                if (station == null) {
                    station = new StationReport(report.stationName);

                    reports.put(station.stationName, station);
                }

                station.setSum(station.getSum() + report.getSum());
                station.setCount(station.getCount() + report.getCount());
                station.setMax(Math.max(station.getMax(), report.getMax()));
                station.setMin(Math.min(station.getMin(), report.getMin()));
            });
        });

        System.out.println("Processed, about to output now.");

        reports.forEach((stationName, report) -> {
            System.out.printf("%s=%.2f/%.2f/%.2f\n", stationName, report.getMin(), (report.getMax() - report.getMin()) / 2, report.getMax());
        });

        System.out.printf("Took %.4f\n", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
    }

    public static class StationReport {
        private final String stationName;
        private double min, sum, max;
        private int count;

        public StationReport(String stationName) {
            this.stationName = stationName;
        }

        public String getStationName() {
            return stationName;
        }

        public double getMin() {
            return min;
        }

        public void setMin(double min) {
            this.min = min;
        }

        public double getMax() {
            return max;
        }

        public void setMax(double max) {
            this.max = max;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }
    }
}
