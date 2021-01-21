package ru.ivi.opensource.flinkclickhousesink.applied;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class NativeClientService implements ClientService {

    private static final Logger logger = LoggerFactory.getLogger(NativeClientService.class);

    public NativeClientService() {
    }


    @Override
    public void send(ClickHouseRequestBlank requestBlank,
                     CompletableFuture<Boolean> future, ClickHouseSinkCommonParams sinkSettings,
                     BlockingQueue<ClickHouseRequestBlank> queue, AtomicLong queueCounter,
                     ExecutorService callbackService) {

        try {
            ClickHouseRunnable runnable = new ClickHouseRunnable(requestBlank, sinkSettings.getClickHouseClusterSettings());
            responseCallback(runnable, requestBlank, future, sinkSettings, queue, queueCounter);
        } catch (Exception ex) {
            logger.error("发送数据时出现异常");
            future.complete(false);
        }
        queueCounter.decrementAndGet();
    }

    @Override
    public void close() throws IOException {

    }


    private void responseCallback(ClickHouseRunnable runnable,
                                  ClickHouseRequestBlank requestBlank, CompletableFuture<Boolean> future,
                                  ClickHouseSinkCommonParams sinkSettings,
                                  BlockingQueue<ClickHouseRequestBlank> queue, AtomicLong queueCounter) {

        boolean response = false;
        try {
            response = runnable.execute();

            if (response == false) {
                handleUnsuccessfulResponse(false, requestBlank, future, sinkSettings, queue, queueCounter);
            } else {
                logger.info("Successful send data to ClickHouse, batch size = {}, target table = {}, current attempt = {}",
                        requestBlank.getValues().size(),
                        requestBlank.getTargetTable(),
                        requestBlank.getAttemptCounter());
                future.complete(true);
            }
        } catch (Exception e) {
            logger.error("Error while executing callback, params = {}", sinkSettings, e);
            requestBlank.setException(e);
            try {
                handleUnsuccessfulResponse(response, requestBlank, future, sinkSettings, queue, queueCounter);
            } catch (Exception error) {
                logger.error("Error while handle unsuccessful response", error);
                future.completeExceptionally(error);
            }
        } finally {
            queueCounter.decrementAndGet();
        }

    }

    private void handleUnsuccessfulResponse(boolean response, ClickHouseRequestBlank requestBlank,
                                            CompletableFuture<Boolean> future,
                                            ClickHouseSinkCommonParams sinkSettings,
                                            BlockingQueue<ClickHouseRequestBlank> queue, AtomicLong queueCounter) throws Exception {
        int currentCounter = requestBlank.getAttemptCounter();
        if (currentCounter >= sinkSettings.getMaxRetries()) {
            logger.warn("Failed to send data to ClickHouse, cause: limit of attempts is exceeded." +
                    " ClickHouse response = {}. Ready to flush data on disk.", response, requestBlank.getException());
            logFailedRecords(requestBlank, sinkSettings);
            future.completeExceptionally(new RuntimeException(String.format("Failed to send data to ClickHouse, cause: limit of attempts is exceeded." +
                    " ClickHouse response: %s. Cause: %s", null, requestBlank.getException())));
        } else {
            requestBlank.incrementCounter();
            logger.warn("Next attempt to send data to ClickHouse, table = {}, buffer size = {}, current attempt num = {}, max attempt num = {}, response = {}",
                    requestBlank.getTargetTable(),
                    requestBlank.getValues().size(),
                    requestBlank.getAttemptCounter(),
                    sinkSettings.getMaxRetries(),
                    response);
            queueCounter.incrementAndGet();
            queue.put(requestBlank);
            future.complete(false);
        }
    }

    private void logFailedRecords(ClickHouseRequestBlank requestBlank,
                                  ClickHouseSinkCommonParams sinkSettings) throws Exception {
        String filePath = String.format("%s/%s_%s",
                sinkSettings.getFailedRecordsPath(),
                requestBlank.getTargetTable(),
                System.currentTimeMillis());

        try (PrintWriter writer = new PrintWriter(filePath)) {
            List<JSONObject> records = (List<JSONObject>) (List) requestBlank.getValues();
            records.forEach(writer::println);
            writer.flush();
        }
        logger.info("Successful send data on disk, path = {}, size = {} ", filePath, requestBlank.getValues().size());
    }
}
