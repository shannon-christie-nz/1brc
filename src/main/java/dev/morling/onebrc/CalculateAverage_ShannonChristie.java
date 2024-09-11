/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.*;

/*
 * Everything is run on the exact same measurements.txt
 * Machine is Ryzen 7 5800X, 32 GB ram at 3600mhz (downclock from 3800mhz), PCI-e 4 M.2 NVMe 2tb Samsung 980 pro
 * Running Elementary OS, built and using IntelliJ with JDK 21 x64.
 *
 * Baseline run 1 on my machine - Took ~158 seconds
 * Baseline run 2 on my machine - Took ~170 seconds
 * Baseline run 3 on my machine - Took ~152 seconds
 *
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
 * 37091ca3524816d763e55697a12607420f9d3228 - "". FileInputChannel, to 100m bytes buffer. Workers decode buffer and work directly. No Strings for new lines. Took ~56 seconds.
 * 57818dec1507fd3908441bec6a60ffa5f5f7784e - 1r~7w. "" "" "". Took ~42 seconds.
 * 200c564dc56729dfb7671a2aa53bd287ed51eb6b - 1r~15w. "" "" "". Took ~47 seconds.
 * 4ff30825f45578b171d5cd56ce886e914c03e323 - competition 1r~7w. "". Took ~43 seconds. - No major delays from disk IO.
 * 93caf58f7102f0e882d7f7556c440d3bf6e54944 - "". "". Reduce timeouts. Took ~36 seconds.
 * 2ccc7ce5f50438b11578b41541298c732da212dc - "". "". Reduce timeouts again and reduce buffer size to 10 million. Took ~35 seconds.
 * 9610561e8ff722ebf1bf0acb7a93f769dca5f73e - "". Custom rolled temperature parsing without decoding and parsing. Took ~20 seconds.
 * e27e1a2f93bc4e10325b34587e7cea14fc10621d - "". Custom rolled name parsing without decoding. Took ~15 seconds.
 * 96dcc26897a0a771afd369b493074bb1f4dbcdb4 - 1r15w. "". Took ~12 seconds.
 * c235e14351eb2d8f701dbf208e89fe50c4ecca14 - "". Disable most logging. Took ~12 seconds.
 * 192bd37501fc9067b8bd7df9fd7667af8c7daa39 - "". Replace computeIfAbsent with manual map population. Took ~11 seconds (10.5~)
 * 78563dee4182d9ae17d773f4daeb7b3dcf9ad07f - "". Refactor to "read" once. Took ~10 seconds.
 * 3523b10358c9bf5864e6866efeefdb79062a9b04 - "". Refactor getTemperatureDouble. Took ~10 seconds.
 * */
public class CalculateAverage_ShannonChristie {
    private static volatile boolean readerHasFinished = false;

    private static final Instant start = Instant.now();

    /////////////////////
    /// Configuration ///
    /////////////////////
    private static final int BUFFER_SIZE = 10_000_000;
    private static final LogLevel LOG_LEVEL = LogLevel.INFO;

    //////////////////////////
    /// Auto-configuration ///
    //////////////////////////
    private static final int cores = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), 15));
    private static final int BUFFERS_COUNT = cores * 3;

    public static void main(String[] args) throws Exception {
        List<LockedBuffer> buffers = new ArrayList<LockedBuffer>(BUFFERS_COUNT);
        for (int i = 0; i < BUFFERS_COUNT; i++) {
            buffers.add(new LockedBuffer(BUFFER_SIZE));
        }
        AtomicRingBuffer queue = new AtomicRingBuffer(buffers);

        startReaderThread(queue);

        ArrayList<HashMap<String, StationReport>> inProgressReports = startWorkerThreads(queue);

        processAndOutputReports(inProgressReports);

        System.out.printf("Took %.4f\n", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
    }

    private static void startReaderThread(AtomicRingBuffer queue) {
        Thread readerThread = new Thread(() -> readMeasurementsToQueue(queue), "Reader-0");

        readerThread.start();

        if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
            System.out.println("Reader thread started");
        }
    }

    private static void readMeasurementsToQueue(AtomicRingBuffer queue) {
        try (FileChannel inputFileChannel = FileChannel.open(Path.of("./measurements.txt"), StandardOpenOption.READ)) {
            final int MAX_RETRIES = 1000;
            int retries = 0;
            int readInBytes = 0;
            while (true) {
                Instant readerStart = Instant.now();

                var lockedBuffer = queue.getBuffer();
                if (lockedBuffer == null) {
                    if (retries++ >= MAX_RETRIES) {
                        break;
                    }

                    continue;
                }

                retries = 0;

                try (var byteBufferGuard = lockedBuffer.lock()) {
                    var byteBuffer = byteBufferGuard.buffer();
                    // Ensure we reset buffers after they may have been used
                    byteBuffer.position(0);
                    byteBuffer.limit(byteBuffer.capacity());

                    readInBytes = inputFileChannel.read(byteBuffer);

                    // Let's read backwards to find the last complete line
                    for (int i = byteBuffer.capacity(); i > 0; i--) {
                        if (byteBuffer.get(i - 1) == '\n') {
                            byteBuffer.limit(i); // Reduce limit to last valid line

                            break; // We can move on now.
                        }
                    }

                    if (byteBuffer.limit() != byteBuffer.capacity()) {
                        // We didn't complete a line, we need to track this change for ensuring
                        // the next read works as intended... i.e. continuing at the start of
                        // the incomplete line.

                        // Position returns the channel itself. It's not creating a new one.
                        inputFileChannel.position(inputFileChannel.position() -
                                (byteBuffer.capacity() - byteBuffer.limit()));// Seek back the difference
                    }

                    if (LOG_LEVEL.ordinal() <= LogLevel.TRACE.ordinal()) {
                        System.out.printf("Reader: read in %.4f seconds\n", (Instant.now().toEpochMilli() - readerStart.toEpochMilli()) / 1000.0);
                    }

                    queue.readyItem();

                    if (readInBytes < BUFFER_SIZE) {
                        if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
                            System.out.printf("Reader: finished read at %d\n", inputFileChannel.position());
                        }

                        break;
                    }
                }
            }

            if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
                System.out.printf("Reader: finished at %.2f\n", (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
            }
        }
        catch (IOException ex) {
            System.err.println("Reader: error reading file");

            System.err.println(ex.getMessage());
        }
        finally {
            readerHasFinished = true;
        }
    }

    private static ArrayList<HashMap<String, StationReport>> startWorkerThreads(AtomicRingBuffer queue) {
        CountDownLatch threadProcessingCompletionLatch = new CountDownLatch(cores);

        ArrayList<HashMap<String, StationReport>> inProgressReports = new ArrayList<>(cores);

        for (int threadI = 0; threadI < cores; threadI++) {
            inProgressReports.add(new HashMap<>());

            final int THREAD_INDEX = threadI;

            Thread t = new Thread(() -> {
                HashMap<String, StationReport> threadSpecificReport = inProgressReports.get(THREAD_INDEX);

                try {
                    while (true) {
                        LockedBuffer lockedBuffer = queue.getItem();

                        if (lockedBuffer == null) {
                            if (LOG_LEVEL.ordinal() <= LogLevel.TRACE.ordinal()) {
                                System.out.printf("Thread %d: failed to get data\n", THREAD_INDEX);
                            }

                            if (readerHasFinished) {
                                if (LOG_LEVEL.ordinal() <= LogLevel.ERROR.ordinal()) {
                                    System.out.printf("Thread %d: leaving\n", THREAD_INDEX);
                                }

                                break;
                            }

                            continue;
                        }

                        if (LOG_LEVEL.ordinal() <= LogLevel.TRACE.ordinal()) {
                            System.out.printf("Thread %d: got work item\n", THREAD_INDEX);
                        }

                        Instant workerStart = Instant.now();

                        try (var bufferGuard = lockedBuffer.lock()) {
                            var buffer = bufferGuard.buffer();

                            final int bufferLimit = buffer.limit();
                            int lastIndex = 0;
                            int delimiterIndex = 0;
                            for (int i = 0; i < bufferLimit; i++) {
                                // Walk through, track the most recent delimiter, and the last
                                // successful new lines end index.
                                try {
                                    byte currentByte = buffer.get(i);

                                    if (currentByte == ';') {
                                        delimiterIndex = i; // Track delimiter
                                    }
                                    else if (currentByte == '\n') { // Got a new line
                                        // We expect that all lines are valid in terms of having
                                        // a station name and a temperature.
                                        String stationName = getStationNameString(delimiterIndex, lastIndex, buffer);
                                        double temperature = getTemperatureDouble(buffer, delimiterIndex, i);

                                        StationReport report = threadSpecificReport.get(stationName);

                                        if (report == null) {
                                            report = new StationReport(stationName);

                                            threadSpecificReport.put(stationName, report);
                                        }

                                        report.sum = (report.sum + temperature);
                                        report.count = (report.count + 1);
                                        report.max = (Math.max(report.max, temperature));
                                        report.min = (Math.min(report.min, temperature));

                                        lastIndex = i + 1;
                                    }
                                }
                                catch (NumberFormatException e) {
                                    System.err.printf("Error parsing temperature in line: %s\n",
                                            buffer.slice(lastIndex, i).toString());
                                }
                            }
                        }

                        if (LOG_LEVEL.ordinal() <= LogLevel.TRACE.ordinal()) {
                            System.out.printf("Worker %d: completed work item in %.2f seconds\n", THREAD_INDEX,
                                    (Instant.now().toEpochMilli() - workerStart.toEpochMilli()) / 1000.0);
                        }
                    }
                }
                finally {
                    // We completed all of our data, or something else went wrong
                    threadProcessingCompletionLatch.countDown();
                }
            }, "Worker-" + THREAD_INDEX);

            t.start();
        }

        if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
            System.out.printf("All threads spawned. %d threads\n", cores);
        }

        try {
            if (!threadProcessingCompletionLatch.await(180, TimeUnit.SECONDS)) {
                throw new RuntimeException("Timed out waiting for thread processing completion.");
            }
        }
        catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for thread processing completion");

            throw new RuntimeException(e);
        }

        return inProgressReports;
    }

    private static String getStationNameString(int delimiterIndex, int lastIndex, ByteBuffer buffer) {
        int length = delimiterIndex - lastIndex;

        char[] charArrayBuffer = new char[length];

        for (int j = 0; j < length; j++) {
            charArrayBuffer[j] = (char) buffer.get(lastIndex + j);
        }

        return String.valueOf(charArrayBuffer);
    }

    private static double getTemperatureDouble(ByteBuffer buffer, int delimiterIndex, int i) {
        int temperature = 0;
        boolean negative;
        int start;
        int length;

        if (buffer.get(delimiterIndex + 1) == '-') {
            negative = true;
            start = delimiterIndex + 2;
            length = i - delimiterIndex - 2;
        }
        else {
            negative = false;
            start = delimiterIndex + 1;
            length = i - delimiterIndex - 1;
        }

        for (int j = 0; j < length; j++) {
            byte value = buffer.get(start + j);

            if (value == '.') {
                continue;
            }

            temperature = temperature * 10 + (value - '0');
        }

        if (negative) {
            temperature = -temperature;
        }

        return temperature / 10.0;
    }

    private static void processAndOutputReports(ArrayList<HashMap<String, StationReport>> inProgressReports) {
        if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
            System.out.println("Processing the data");
        }

        HashMap<String, StationReport> reports = new HashMap<>(10_000);

        inProgressReports.forEach((threadMap) -> {
            if (LOG_LEVEL.ordinal() <= LogLevel.TRACE.ordinal()) {
                System.out.println("Got thread map, processing");
            }

            threadMap.forEach((ignored, report) -> {
                StationReport station = reports.get(report.stationName);

                if (station == null) {
                    station = new StationReport(report.stationName);

                    reports.put(station.stationName, station);
                }

                station.sum = (station.sum + report.sum);
                station.count = (station.count + report.count);
                station.max = (Math.max(station.max, report.max));
                station.min = (Math.min(station.min, report.min));
            });
        });

        if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
            System.out.println("Processed, about to output now.");
        }

        reports.forEach((stationName, report) -> System.out.printf("%s=%.2f/%.2f/%.2f\n", stationName, report.min, report.sum / report.count, report.max));
    }

    public static class StationReport {
        private final String stationName;
        private double min = 99.9, sum, max = -99.9;
        private int count;

        public StationReport(String stationName) {
            this.stationName = stationName;
        }
    }

    /**
     * AtomicRingBuffer provides a mostly lockless ring buffer.
     * 
     * Much of the behaviour for getting the next valid buffer
     * from both the consumer and producer utilises atomics,
     * avoiding excessive locking. 
     * 
     * However, the access to the buffer itself is guarded. 
     * This guarding will at most be between the producer and 
     * a single consumer. Thus keeping the lock contention to 
     * minimum.
     * 
     * NOTE this is intended for single producer, multiple consumer.
     */
    public static class AtomicRingBuffer {
        private final AtomicInteger producerIndex;
        private final AtomicInteger consumerIndex;
        private final List<LockedBuffer> buffers;
        private final int maxSize;

        public AtomicRingBuffer(List<LockedBuffer> buffers) {
            this.buffers = buffers;
            this.maxSize = buffers.size();
            
            this.producerIndex = new AtomicInteger(0);
            this.consumerIndex = new AtomicInteger(this.maxSize);
        }

        /**
         * Gets the next valid buffer for use by the producer.
         * 
         * You must call readyItem when complete.
         * 
         * @return
         */
        public LockedBuffer getBuffer() {
            var consumerCurrent = consumerIndex.getAcquire();
            var producerCurrent = producerIndex.getAcquire();

            if (producerCurrent == consumerCurrent) { // Stop if index matches, no room
                return null;
            }

            return this.buffers.get(producerCurrent);
        }

        /** Readies an item for consumers */
        public void readyItem() {
            var producerCurrent = producerIndex.getAcquire();
            var newIndex = producerCurrent + 1;

            if (newIndex >= maxSize) { // Wrap if reaching boundary
                newIndex = 0;
            }

            producerIndex.compareAndExchangeAcquire(producerCurrent, newIndex);
        }

        /**
         * Gets the next readied item for consumption.
         * 
         * @return
         */
        public LockedBuffer getItem() {
            var producerCurrent = producerIndex.getAcquire();
            var consumerCurrent = consumerIndex.getAcquire();
            var newIndex = consumerCurrent + 1;

            if (newIndex >= maxSize) { // Wrap if reaching boundary
                newIndex = 0;
            }

            if (newIndex == producerCurrent) { // Stop if index matches, no work
                return null;
            }

            if (consumerCurrent != consumerIndex.compareAndExchangeAcquire(consumerCurrent, newIndex)) {
                return null; // Failed to acquire new index
            }

            return this.buffers.get(newIndex); // Return item, we got it
        }
    }

    /**
     * LockedBuffer is a "mutex" protected buffer.
     * 
     * After obtaining an instance of LockedBuffer,
     * you must call the `lock()` method to acquire
     * the lock and buffer via a LockedBufferGuard.
     * 
     * @see LockedBufferGuard
     */
    public static class LockedBuffer {
        private static int counter = 0;
        private static final LogLevel LOG_LEVEL = LogLevel.NONE;

        private ByteBuffer buffer;
        private Lock lock;

        private int lockId = LockedBuffer.counter++;

        public LockedBuffer(int bufferSize) {
            this.buffer = ByteBuffer.allocate(bufferSize);
            this.lock = new ReentrantLock();
        }

        /**
         * Acquires the lock and returns the guard.
         * 
         * Best used within a try with resources.
         * 
         * @return
         */
        public LockedBufferGuard lock() {
            if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
                System.out.printf("Thread %s: acquiring lock %d\n", Thread.currentThread().getName(), lockId);
            }

            this.lock.lock();

            if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
                System.out.printf("Thread %s: acquired lock %d\n", Thread.currentThread().getName(), lockId);
            }

            return new LockedBufferGuard(this, buffer);
        }

        /**
         * Release will be called by LockedBufferGuard
         */
        private void release() {
            if (LOG_LEVEL.ordinal() <= LogLevel.INFO.ordinal()) {
                System.out.printf("Thread %s: releasing lock %d\n", Thread.currentThread().getName(), lockId);
            }

            this.lock.unlock();
        }

        /**
         * LockedBufferGuard is best used in a try
         * with resources block as it implements
         * AutoCloseable to help release the lock.
         * 
         * Call `buffer()` to get the buffer.
         * 
         * DO NOT USE THE BUFFER AFTER CLOSING.
         */
        public static class LockedBufferGuard implements AutoCloseable {
            private final LockedBuffer guarded;
            private final ByteBuffer buffer;

            private LockedBufferGuard(LockedBuffer guarded, ByteBuffer buffer) {
                this.guarded = guarded;
                this.buffer = buffer;
            }

            public ByteBuffer buffer() {
                // Ideally we track if this lock has been released.
                // For example, someone called `close()` manually.
                // That should prevent further reading of this buffer.
                return buffer;
            }

            @Override
            public void close() {
                this.guarded.release();
            }
        }
    }

    public enum LogLevel {
        TRACE,
        INFO,
        WARNING,
        ERROR,
        NONE,
    }
}
