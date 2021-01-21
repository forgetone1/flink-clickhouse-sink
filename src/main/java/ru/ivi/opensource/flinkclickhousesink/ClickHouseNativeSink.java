package ru.ivi.opensource.flinkclickhousesink;

import com.alibaba.fastjson.JSONObject;
import ru.ivi.opensource.flinkclickhousesink.applied.NativeClientService;
import ru.ivi.opensource.flinkclickhousesink.model.ClientProtocol;

import java.util.Properties;

public class ClickHouseNativeSink extends AbstractClickHouseSink<JSONObject> {

    public ClickHouseNativeSink(Properties properties) {
        super(properties, new NativeClientService(), ClientProtocol.TCP);
    }
}
