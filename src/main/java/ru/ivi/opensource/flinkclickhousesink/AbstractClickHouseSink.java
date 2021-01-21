package ru.ivi.opensource.flinkclickhousesink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseSinkManager;

import ru.ivi.opensource.flinkclickhousesink.applied.ClientService;
import ru.ivi.opensource.flinkclickhousesink.applied.Sink;
import ru.ivi.opensource.flinkclickhousesink.model.ClientProtocol;

import java.util.Map;
import java.util.Properties;

abstract class AbstractClickHouseSink<T> extends RichSinkFunction<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractClickHouseSink.class);

    private static final Object DUMMY_LOCK = new Object();

    private final Properties localProperties;

    private volatile static transient ClickHouseSinkManager sinkManager;
    private transient Sink sink;
    private final ClientService clientService;
    private final ClientProtocol clientProtocol;

    public AbstractClickHouseSink(Properties properties, ClientService clientService, ClientProtocol clientProtocol) {
        this.localProperties = properties;
        this.clientService = clientService;
        this.clientProtocol = clientProtocol;
    }

    @Override
    public void open(Configuration config) {
        if (sinkManager == null) {
            synchronized (DUMMY_LOCK) {
                if (sinkManager == null) {
                    Map<String, String> params = getRuntimeContext()
                            .getExecutionConfig()
                            .getGlobalJobParameters()
                            .toMap();

                    sinkManager = new ClickHouseSinkManager(params, this.clientService, this.clientProtocol);
                }
            }
        }

        sink = sinkManager.buildSink(localProperties);
    }

    /**
     * Add csv to sink
     *
     * @param record csv-event
     */
    @Override
    public void invoke(T record, SinkFunction.Context context) {
        try {
            sink.put(record);
        } catch (Exception e) {
            logger.error("Error while sending data to ClickHouse, record = {}", record, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (sink != null) {
            sink.close();
        }

        if (sinkManager != null) {
            if (!sinkManager.isClosed()) {
                synchronized (DUMMY_LOCK) {
                    if (!sinkManager.isClosed()) {
                        sinkManager.close();
                    }
                }
            }
        }

        super.close();
    }
}
