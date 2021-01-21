package ru.ivi.opensource.flinkclickhousesink.applied;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.asynchttpclient.*;
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

public class AsyncHttpClientService implements ClientService {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseWriter.WriterTask.class);

    private static final int HTTP_OK = 200;
    protected final AsyncHttpClient asyncHttpClient;

    public AsyncHttpClientService() {
        this.asyncHttpClient = Dsl.asyncHttpClient();
    }


    public void send(ClickHouseRequestBlank requestBlank,
                     CompletableFuture<Boolean> future, ClickHouseSinkCommonParams sinkSettings,
                     BlockingQueue<ClickHouseRequestBlank> queue, AtomicLong queueCounter,
                     ExecutorService callbackService) {
        Request request = buildRequest(requestBlank, sinkSettings);
        logger.info("Ready to load data to {}, size = {}", requestBlank.getTargetTable(), requestBlank.getValues().size());
        ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(request);
        Runnable callback = responseCallback(whenResponse, requestBlank, future, sinkSettings, queueCounter, queue);
        whenResponse.addListener(callback, callbackService);
    }

    @Override
    public void close() throws IOException {
        this.asyncHttpClient.close();
    }

    private Request buildRequest(ClickHouseRequestBlank requestBlank, ClickHouseSinkCommonParams sinkSettings) {
        String resultCSV = String.join(" , ", (List<String>) (List) requestBlank.getValues());
        String query = String.format("INSERT INTO %s VALUES %s", requestBlank.getTargetTable(), resultCSV);
        String host = sinkSettings.getClickHouseClusterSettings().getRandomHostUrl();

        BoundRequestBuilder builder = asyncHttpClient
                .preparePost(host)
                .setHeader(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8")
                .setBody(query);

        if (sinkSettings.getClickHouseClusterSettings().isAuthorizationRequired()) {
            builder.setHeader(HttpHeaderNames.AUTHORIZATION, "Basic " + sinkSettings.getClickHouseClusterSettings().getCredentials());
        }

        return builder.build();
    }

    private Runnable responseCallback(ListenableFuture<Response> whenResponse, ClickHouseRequestBlank requestBlank,
                                      CompletableFuture<Boolean> future, ClickHouseSinkCommonParams sinkSettings,
                                      AtomicLong queueCounter, BlockingQueue<ClickHouseRequestBlank> queue) {
        return () -> {
            Response response = null;
            try {
                response = whenResponse.get();

                if (response.getStatusCode() != HTTP_OK) {
                    handleUnsuccessfulResponse(response, requestBlank, future, sinkSettings, queueCounter, queue);
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
                    handleUnsuccessfulResponse(response, requestBlank, future, sinkSettings, queueCounter, queue);
                } catch (Exception error) {
                    logger.error("Error while handle unsuccessful response", error);
                    future.completeExceptionally(error);
                }
            } finally {
                queueCounter.decrementAndGet();
            }
        };
    }

    private void handleUnsuccessfulResponse(Response response, ClickHouseRequestBlank requestBlank,
                                            CompletableFuture<Boolean> future,
                                            ClickHouseSinkCommonParams sinkSettings,
                                            AtomicLong queueCounter,
                                            BlockingQueue<ClickHouseRequestBlank> queue
    ) throws Exception {
        int currentCounter = requestBlank.getAttemptCounter();
        if (currentCounter >= sinkSettings.getMaxRetries()) {
            logger.warn("Failed to send data to ClickHouse, cause: limit of attempts is exceeded." +
                    " ClickHouse response = {}. Ready to flush data on disk.", response, requestBlank.getException());
            logFailedRecords(requestBlank, sinkSettings);
            future.completeExceptionally(new RuntimeException(String.format("Failed to send data to ClickHouse, cause: limit of attempts is exceeded." +
                    " ClickHouse response: %s. Cause: %s", response != null ? response.getResponseBody() : null, requestBlank.getException())));
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
            List<Object> records = requestBlank.getValues();
            records.forEach(writer::println);
            writer.flush();
        }
        logger.info("Successful send data on disk, path = {}, size = {} ", filePath, requestBlank.getValues().size());
    }

}
