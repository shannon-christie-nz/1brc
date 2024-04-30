package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * 382743987775cd6ca97ee77cbfb20ee09ca8398b - 1r1w. RandomAccessFile + Memory Mapped v1. Took ~153 seconds.
 * a72fa41407128babb10a8f441f4f5a3b03f138ed - 1r1w. RandomAccessFile + Memory Mapped v1. Refactor. Took ~141 seconds.
 * f78d737d0181c1c42a1a067210da76d5701af7a7 - 1r1w. BufferedReader again. Took ~68 seconds.
 * 0fbbfdb2717a59fd25ce6f30721732ce4fde075a - 1r1w. BufferedReader, 1 million per batch. Took ~91 seconds.
 * 4c3793c44cca3da29013138e19f09637465fef0a - 1r1w. BR, 10 million per batch. Took ~69 seconds.
 * f722624b8c3ffa63c2a7b1936bcfec5846a79f08 - 1r~4w. BR, 10 million/batch. Took ~68 seconds.
 * 5efb0d63ebe89adc77830773dcf911fc0dc78f4d - 1r~4w. BR, Read to buffer 10m bytes. Took ~120 seconds.
 * 368ff01cbfa3777d55377cb923b09f02a679c033 - "". "", "" 100m bytes. Took ~117 seconds
 * */
public class CalculateAverage_ShannonChristie {
    public static boolean readerHasFinished = false;

    public static void main(String[] args) {
        Instant start = Instant.now();

        /////////////////////
        /// Configuration ///
        /////////////////////
        final int BATCH_SIZE = 100_000_000;
        final int READER_TIMEOUT = 20;
        final int WORKER_TIMEOUT = 6;

        //////////////////////////
        /// Auto-configuration ///
        //////////////////////////
        Runtime runtime = Runtime.getRuntime();
        int cores = Math.max(1, Math.min(runtime.availableProcessors() / 2, 4));

        LinkedBlockingQueue<ArrayList<String>> queue = new LinkedBlockingQueue<>(cores);

        startReaderThread(BATCH_SIZE, queue, READER_TIMEOUT, start);

        ArrayList<ConcurrentHashMap<String, StationReport>> inProgressReports =
                startWorkerThreads(cores, queue, WORKER_TIMEOUT);

        processAndOutputReports(inProgressReports);

        System.out.printf("Took %.4f\n", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
    }

    private static ArrayList<ConcurrentHashMap<String, StationReport>> startWorkerThreads(int cores, LinkedBlockingQueue<ArrayList<String>> queue, int WORKER_TIMEOUT) {
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
                        ArrayList<String> list = queue.poll(WORKER_TIMEOUT, TimeUnit.SECONDS);

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
        return inProgressReports;
    }

    private static void processAndOutputReports(ArrayList<ConcurrentHashMap<String, StationReport>> inProgressReports) {
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
    }

    private static void startReaderThread(int BATCH_SIZE, LinkedBlockingQueue<ArrayList<String>> queue, int READER_TIMEOUT, Instant start) {
        Thread readerThread = new Thread(() ->
            readMeasurementsToQueue(BATCH_SIZE, queue, READER_TIMEOUT, start));

        readerThread.start();

        System.out.println("Reader thread started");
    }

    private static void readMeasurementsToQueue(int BATCH_SIZE, LinkedBlockingQueue<ArrayList<String>> queue, int READER_TIMEOUT, Instant start) {
        try (BufferedReader reader = Files.newBufferedReader(Path.of("./measurements.txt"))) {
            CharBuffer charBuffer = CharBuffer.allocate(BATCH_SIZE);
            String carryOverLine = null;

            int read;
            while ((read = reader.read(charBuffer)) != -1) {
                Instant readerStart = Instant.now();

                ArrayList<String> lines = new ArrayList<>(BATCH_SIZE);

                int lastRead = 0;
                for (int i = 0; i < read; i++) {
                    if (charBuffer.get(i) == '\n') {
                        CharBuffer lineBuffer = charBuffer.slice(lastRead, i - lastRead);

                        if (carryOverLine == null) {
                            lines.add(lineBuffer.toString());
                        } else {
                            lines.add(carryOverLine + lineBuffer.toString());
                            carryOverLine = null;
                        }

                        lastRead = i + 1;
                    }
                }

                if (lastRead != read) {
                    // We didn't complete a line
                    carryOverLine = charBuffer.slice(lastRead, read - lastRead).toString();
                }

                System.out.printf("Reader: read %d lines in %.2f seconds\n", lines.size(), (Instant.now().toEpochMilli() - readerStart.toEpochMilli()) / 1000.0);

                // If workers can't complete a batch in 20 seconds when we start to block
                // something must've gone wrong.
                queue.offer(lines, READER_TIMEOUT, TimeUnit.SECONDS);

                charBuffer.clear();
            }

            System.out.printf("Reader: finished at %.2f\n", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
        } catch (IOException ex) {
            System.err.println("Reader: error reading file");

            System.err.println(ex.getMessage());
        } catch (InterruptedException ex) {
            System.err.println("Reader: workers couldn't process fast enough, we timed out at 20 seconds");

            System.err.println(ex.getMessage());
        } finally {
            readerHasFinished = true;
        }
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
