package ru.ivi.opensource.flinkclickhousesink.applied;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.util.FutureUtil;
import ru.ivi.opensource.flinkclickhousesink.util.ThreadUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ClickHouseWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseWriter.class);
    private ExecutorService service;
    private ExecutorService callbackService;
    private List<WriterTask> tasks;
    private final BlockingQueue<ClickHouseRequestBlank> commonQueue;
    private final AtomicLong unprocessedRequestsCounter = new AtomicLong();
    private final List<CompletableFuture<Boolean>> futures;
    private final ClickHouseSinkCommonParams sinkParams;
    private final ClientService clientService;

    public ClickHouseWriter(ClickHouseSinkCommonParams sinkParams, List<CompletableFuture<Boolean>> futures, ClientService clientService) {
        this.sinkParams = sinkParams;
        this.futures = futures;
        this.commonQueue = new LinkedBlockingQueue<>(sinkParams.getQueueMaxCapacity());
        this.clientService = clientService;
        initDirAndExecutors();
    }

    private void initDirAndExecutors() {
        try {
            initDir(sinkParams.getFailedRecordsPath());
            buildComponents();
        } catch (Exception e) {
            logger.error("Error while starting CH writer", e);
            throw new RuntimeException(e);
        }
    }

    private static void initDir(String pathName) throws IOException {
        Path path = Paths.get(pathName);
        Files.createDirectories(path);
    }

    private void buildComponents() {
        logger.info("Building components");

        ThreadFactory threadFactory = ThreadUtil.threadFactory("clickhouse-writer");
        service = Executors.newFixedThreadPool(sinkParams.getNumWriters(), threadFactory);

        ThreadFactory callbackServiceFactory = ThreadUtil.threadFactory("clickhouse-writer-callback-executor");

        int cores = Runtime.getRuntime().availableProcessors();
        int coreThreadsNum = Math.max(cores / 4, 2);
        callbackService = new ThreadPoolExecutor(
                coreThreadsNum,
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                callbackServiceFactory);


        int numWriters = sinkParams.getNumWriters();
        tasks = Lists.newArrayListWithCapacity(numWriters);
        for (int i = 0; i < numWriters; i++) {
            WriterTask task = new WriterTask(i, commonQueue, sinkParams, callbackService, futures, unprocessedRequestsCounter, this.clientService);
            tasks.add(task);
            service.submit(task);
        }
    }

    public void put(ClickHouseRequestBlank params) {
        try {
            unprocessedRequestsCounter.incrementAndGet();
            commonQueue.put(params);
        } catch (InterruptedException e) {
            logger.error("Interrupted error while putting data to queue", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitUntilAllFuturesDone() {
        logger.info("Wait until all futures are done or completed exceptionally. Futures size: {}", futures.size());
        try {
            while (unprocessedRequestsCounter.get() > 0 || !futures.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Futures size: {}.", futures.size());
                }
                CompletableFuture<Void> future = FutureUtil.allOf(futures);
                try {
                    future.get();
                    futures.removeIf(f -> f.isDone() && !f.isCompletedExceptionally());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Futures size after removing: {}", futures.size());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            stopWriters();
            futures.clear();
        }
    }

    private void stopWriters() {
        logger.info("Stopping writers.");
        if (tasks != null && tasks.size() > 0) {
            tasks.forEach(WriterTask::setStopWorking);
        }
        logger.info("Writers stopped.");
    }

    @Override
    public void close() throws Exception {
        logger.info("ClickHouseWriter is shutting down.");
        try {
            waitUntilAllFuturesDone();
        } finally {
            ThreadUtil.shutdownExecutorService(service);
            ThreadUtil.shutdownExecutorService(callbackService);
            this.clientService.close();
            logger.info("{} shutdown complete.", ClickHouseWriter.class.getSimpleName());
        }
    }

    static class WriterTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(WriterTask.class);

        private static final int HTTP_OK = 200;

        private final BlockingQueue<ClickHouseRequestBlank> queue;
        private final AtomicLong queueCounter;
        private final ClickHouseSinkCommonParams sinkSettings;
        private final ClientService clientService;
        private final ExecutorService callbackService;
        private final List<CompletableFuture<Boolean>> futures;

        private final int id;

        private volatile boolean isWorking;

        WriterTask(int id,
                   BlockingQueue<ClickHouseRequestBlank> queue,
                   ClickHouseSinkCommonParams settings,
                   ExecutorService callbackService,
                   List<CompletableFuture<Boolean>> futures,
                   AtomicLong queueCounter,
                   ClientService clientService) {
            this.id = id;
            this.sinkSettings = settings;
            this.queue = queue;
            this.callbackService = callbackService;
            this.futures = futures;
            this.queueCounter = queueCounter;
            this.clientService = clientService;
        }

        @Override
        public void run() {
            try {
                isWorking = true;

                logger.info("Start writer task, id = {}", id);
                while (isWorking || queue.size() > 0) {
                    ClickHouseRequestBlank blank = queue.poll(300, TimeUnit.MILLISECONDS);
                    if (blank != null) {
                        CompletableFuture<Boolean> future = new CompletableFuture<>();
                        futures.add(future);
                        this.clientService.send(blank, future, sinkSettings, queue, queueCounter, callbackService);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while inserting data", e);
                throw new RuntimeException(e);
            } finally {
                logger.info("Task id = {} is finished", id);
            }
        }


        void setStopWorking() {
            isWorking = false;
        }
    }
}