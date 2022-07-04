package kz.hustle.tools.sort;

/*import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.error.DMCError;*/
import org.apache.avro.generic.GenericRecord;

import java.sql.*;
import java.util.LinkedList;
import java.util.Properties;

public class DMCMemoryData {

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private int currentBatch = 0;
    private boolean isBatchEmpty = true;
    private boolean isInitCache = false;
    private String classType = null;
    private String sortColumn = null;

    public DMCMemoryData() {
    }

    public void setSortColumn(String sortColumn) {
        this.sortColumn = sortColumn;
    }

    public Connection getConnection() {
        return connection;
    }

    public synchronized DMCMemoryData connectToCache() throws ClassNotFoundException, InterruptedException {
        Class.forName("org.sqlite.JDBC");
        Properties properties = new Properties();
        properties.setProperty("reWriteBatchedInserts", "true");
        String controlUrl = "jdbc:sqlite:file::memory:?cache=shared&journal_mode=OFF&synchronous=OFF&reWriteBatchedInserts=true";
        retry(() -> {
            connection = DriverManager.getConnection(controlUrl, properties);
            connection.setAutoCommit(false);
        });
        return this;
    }

    public void creteCache() throws SQLException, InterruptedException {
        retry(() -> {
            connection.createStatement().execute("CREATE TABLE IF NOT EXISTS CACHE(KEY TEXT, DATA BLOB)");
            connection.commit();
        });
        preparedStatement = connection.prepareStatement("INSERT INTO CACHE VALUES (?, ?)");
    }

    public synchronized void disconnectFromCache() throws SQLException, InterruptedException {
        connection.close();
    }

    public void appendData(LinkedList<GenericRecord> records) throws Exception {
        if (sortColumn != null) {
            appendDataWithSort(records);
        } else {
            appendDataWithoutSort(records);
        }
    }

    private void appendDataWithSort(LinkedList<GenericRecord> records) throws Exception {
        Object _val = null;
        for (GenericRecord record : records) {
            _val = record.get(sortColumn);
            if (_val == null) {
                throw new Exception("Sort column value can not be empty");
            }
            String valS = String.valueOf(record.get(sortColumn));
            appendData(valS, ByteStreamCompressor.compressGzip(ParquetGenericRecordBinary.recordToBytes(record, record.getSchema())));
        }
        retry(this::executeBatch);
    }

    private void appendDataWithoutSort(LinkedList<GenericRecord> records) throws Exception {
        for (GenericRecord record : records) {
            appendData(null, ByteStreamCompressor.compressGzip(ParquetGenericRecordBinary.recordToBytes(record, record.getSchema())));
        }
        retry(this::executeBatch);
    }

    private void appendData(Object key, byte[] data) throws SQLException, InterruptedException {
        currentBatch++;

        if (key instanceof String) {
            preparedStatement.setString(1, (String) key);
        } else if (key instanceof Long) {
            preparedStatement.setLong(1, (Long) key);
        } else preparedStatement.setNull(1, Types.OTHER);

        preparedStatement.setBytes(2, data);

        preparedStatement.addBatch();
    }

    private void appendData(Object key, String data) throws SQLException, InterruptedException {
        currentBatch++;

        if (key instanceof String) {
            preparedStatement.setString(1, (String) key);
        } else if (key instanceof Long) {
            preparedStatement.setLong(1, (Long) key);
        }

        preparedStatement.setString(2, data);

        preparedStatement.addBatch();

    }

    private void executeBatch() throws SQLException, InterruptedException {
        synchronized (DMCMemoryData.class) {
            preparedStatement.executeBatch();
            connection.commit();
            preparedStatement.clearBatch();
            preparedStatement.close();
        }

    }

    private void retry(Attempt attempt) throws InterruptedException {
        boolean _isError = false;
        do {
            try {
                attempt.execute();
                _isError = false;
            } catch (Exception e) {
                //String _exception = DMCError.get().getShortErrorText(e);
                String _exception = e.getMessage();
                if (_exception.contains("[SQLITE_LOCKED_SHAREDCACHE]")) {
                    _isError = true;
                    //System.out.println(DMCConsoleColors.colorYellowText("Перезапуск комманды..."));
                    System.out.println("Command relaunch...");
                    //System.out.println(DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e)));
                    e.printStackTrace();
                    Thread.sleep(10);
                }
            }
        } while (_isError);
    }

    private interface Attempt {
        void execute() throws Exception;
    }
}
