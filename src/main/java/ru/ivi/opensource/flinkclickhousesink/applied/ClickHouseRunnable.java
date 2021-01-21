package ru.ivi.opensource.flinkclickhousesink.applied;

import com.alibaba.fastjson.JSONObject;
import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.jdbc.statement.ClickHousePreparedInsertStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ClickHouseRunnable {
    private ClickHouseRequestBlank requestBlank;
    ClickHouseClusterSettings settings;

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseRunnable.class);

    public ClickHouseRunnable(ClickHouseRequestBlank blank, ClickHouseClusterSettings settings) {
        requestBlank = blank;
        this.settings = settings;
    }

    public boolean execute() {
        try {
            String tableName = String.valueOf(requestBlank.getTargetTable());
            String connectionStr = String.format("jdbc:clickhouse://%s", this.settings.getHostsWithPorts().stream().findFirst().get());
            ClickHouseConnection connection = (ClickHouseConnection) DriverManager.getConnection(connectionStr, this.settings.getUser(), this.settings.getPassword());

            Optional<PreparedStatement> statement = generatePreparedStatement((List<JSONObject>) (List) requestBlank.getValues(), connection, tableName);
            if (statement.isPresent()) {
                return statement.get().executeUpdate() > 0;
            }

        } catch (Exception ex) {
            logger.error("执行写入异常", ex);
        }
        return false;
    }

    private Optional<PreparedStatement> generatePreparedStatement(List<JSONObject> list, ClickHouseConnection connection, String tableName) {
        try {
            JSONObject first = list.stream().findFirst().orElse(new JSONObject());
            if (first.keySet().size() <= 0) {
                return Optional.empty();
            }
            String[] keyArray = first.keySet().toArray(new String[first.keySet().size()]);
            String values = String.join(",", Collections.nCopies(keyArray.length, "?"));
            String keys = String.join(",", keyArray);

            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, keys, values);
            logger.info(sql);
            ClickHousePreparedInsertStatement pstmt = (ClickHousePreparedInsertStatement) connection.prepareStatement(sql);
            for (JSONObject item : list) {
                setStatement(pstmt, connection, item, keyArray);
            }
            return Optional.of(pstmt);
        } catch (Exception ex) {
            logger.error("生成链接异常", ex);
        }
        return Optional.empty();
    }

    private void setStatement(ClickHousePreparedInsertStatement pstmt, ClickHouseConnection connection, JSONObject item, String[] keyArray) throws Exception {
        for (int i = 0; i < keyArray.length; i++) {
            String key = keyArray[i];
            Object val = item.get(key);
            int index = i + 1;

            if (val instanceof LocalDateTime) {
                LocalDateTime tmp = (LocalDateTime) val;
                pstmt.setObject(index, Timestamp.valueOf(tmp));
            } else if (val instanceof Long) {
                pstmt.setLong(index, (long) val);
            } else if (val instanceof Integer) {
                pstmt.setInt(index, (Integer) val);
            } else if (val instanceof Timestamp) {
                pstmt.setObject(index, val);
            } else if (val instanceof List) {
                List<String> tmp = (List<String>) val;
                pstmt.setArray(index, connection.createArrayOf("String", tmp.toArray()));
            } else if (val instanceof String) {
                pstmt.setString(index, String.valueOf(val));
            } else {
                pstmt.setString(index, String.valueOf(val));
            }
        }
        pstmt.addBatch();
    }
}
