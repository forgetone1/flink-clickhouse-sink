package ru.ivi.opensource.flinkclickhousesink;

import ru.ivi.opensource.flinkclickhousesink.applied.AsyncHttpClientService;
import ru.ivi.opensource.flinkclickhousesink.model.ClientProtocol;

import java.util.Properties;


public class ClickHouseSink extends AbstractClickHouseSink<String> {

    public ClickHouseSink(Properties properties) {
        super(properties, new AsyncHttpClientService(), ClientProtocol.HTTP);
    }
}
