package ru.ivi.opensource.flinkclickhousesink.applied;

import java.util.concurrent.ExecutionException;

public interface Sink extends AutoCloseable {
    void put(Object message) throws ExecutionException, InterruptedException;
}
