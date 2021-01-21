package ru.ivi.opensource.flinkclickhousesink.applied;

import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public interface ClientService {

    void send(ClickHouseRequestBlank requestBlank,
              CompletableFuture<Boolean> future, ClickHouseSinkCommonParams sinkSettings,
              BlockingQueue<ClickHouseRequestBlank> queue, AtomicLong queueCounter,
              ExecutorService callbackService);

    void close() throws IOException;
}
