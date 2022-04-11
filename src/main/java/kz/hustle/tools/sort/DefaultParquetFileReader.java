package kz.hustle.tools.sort;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.Closeable;
import java.io.IOException;
import java.security.NoSuchProviderException;
import java.sql.SQLException;

public class DefaultParquetFileReader implements Closeable {
    private ParquetReader<GenericRecord> fileReader = null;
    //private GenericRecord record = null;
    private Configuration conf = null;

    public DefaultParquetFileReader setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public DefaultParquetFileReader open(FileStatus file) throws IOException, SQLException, NoSuchProviderException, ClassNotFoundException {
        fileReader = AvroParquetReader
                .<GenericRecord>builder(getInputFile(file))
                .disableCompatibility()
                .build();
        return this;
    }

    public GenericRecord getRecord() throws IOException {
        return fileReader.read();
    }

    public Schema getSchema() throws IOException {
        GenericRecord _record = getRecord();
        if (_record != null) {
            return _record.getSchema();
        }

        return null;
    }


    /*public DefaultParquetFileReader close() throws IOException {
        fileReader.close();
        return this;
    }*/

    public long getRecordsCount(FileStatus file) throws IOException {
        ParquetFileReader reader = new ParquetFileReader(getInputFile(file), ParquetReadOptions.builder().build());

        long _rowsCount = 0;

        for (BlockMetaData block : reader.getFooter().getBlocks()) {
            _rowsCount += block.getRowCount();
        }

        reader.close();

        return _rowsCount;
    }

    private HadoopInputFile getInputFile(FileStatus file) throws IOException {
        return HadoopInputFile.fromPath(new Path(file.getPath().toString()), conf);
    }

    @Override
    public void close() throws IOException {
        fileReader.close();
    }
}
