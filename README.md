# Parquet Toolkit #
Version 0.0.2 <br />
<br />
Tools for optimization and transformation Apache Parquet files.

## Capabilities ##
- Merging Parquet files into one large file;
- Merging Parquet files into multiple larger files.;
- Sorting Parquet file by schema field;
- Splitting files;
- Converting CSV to Parquet

## Building ##
```
gradlew build
```

## Usage ##

Операции производятся либо с отдельными файлами, либо с каталогами, в которых находятся parquet файлы.

### Создание объекта ParquetFolder ###

Для объединения и укрупнения файлов в определенном каталоге HDFS необходимо создать объект ParquetFolder.

```
ParquetFolder parquetFolder = new ParquetFolder(new Path("/path/in/hdfs"), configuration);
```

### Объединение в один файл ###

Все файлы с одинаковой схемой объединяются в один файл.<br />
Если в каталоге содержатся файлы с разными схемами, то на выходе получается несколько файлов. К имени файла добавляется "-schemaN".

```
TreeMultithreadedParquetMerger.builder(parquetFolder)
.build()
.merge();
```

### Укрупнение в несколько частей ###

Файлы в каталоге объединяются в более крупные файлы.<br />
Размер выходных файлов зависит от переданных аргументов.<br />
Укрупненные файлы по умолчанию сохраняются в каталоге /merged, созданном внутри каталога с входными файлами.

```
ParquetMerger merger = SimpleMultithreadedParquetMerger.builder(parquetFolder)
                .outputRowGroupSize(128 * 1024 * 1024)
                .withInputChunkSize(512 * 1024 * 1024)
                .threadPoolSize(64)
                .compressionCodec(CompressionCodecName.GZIP)
                .outputPath("/path/for/output/files")
                .removeInputFiles(true, false)
                .build();
merger.merge();
```

При создании билдера можно указать следующие опции:
- ```withThreadPoolSize(int delimiter)```: Количество потоков объяединения. Рекомендуется указывать не больше количества доступных процессорных ядер. По-умолчанию 64 потока.
- ```withOutputRowGroupSize(int outputRowGroupSize)```: Размер в байтах RowGroup для выходных parquet файлов. По-умолчанию 134217728 (128Мб).
- ```withInputChunkSize(int inputChunkSize)```: количество байт, обрабатываемых одним потоком. Каждый поток на выходе создает один файл, таким образом, данная опция напрямую влияет на размер выходного файла, однако большое значение имеют также кодеки сжатия исходных и выходных файлов.Если кодеки одинаковы, то размер выходного файла будет совпадать со значением данного параметра. Рекомендуется указывать размер выходного файла примерно равным размеру блока в HDFS. По умолчанию 134217728 (128Мб).
- ```withCompressionCodec(CompressionCodecName compressionCodecName)```: Кодек сжатия для выходных файлов.
- ```withOutputPath(String outputPath)```: Путь в HDFS для выходных файлов.
- ```withRemoveInputFiles()```: Удалять исходные файлы при успешном объединении. Если сохранение выходных файлов происходит в тот же каталог, в котором хранятся исходные файлы, то удаление исходных файлов происходит вне зависимости от значения данной опции.
- ```withRemoveInputDir()```: Удалять исходный каталог при успешном объединении.
- ```withOutputFileName(Strin outputFileName)```: Изменить имя выходного файла. По-умолчанию "merged-datafile.parq"
- ```withBadBlockReadAttempts(int badBlockReadAttempts)```: Количество попыток чтения файла с hdfs при недоступности блока. По-умолчанию 5. Не рекомендуется менять, кроме случаев когда HDFS работает крайне нестабильно и наблюдается частая недоступность нод.
- ```withBadBlockReadTimeout(long badBlockReadTimeout)```: Задержка в милисекундах между попытками чтения с hdfs при недоступности блока. По-умолчанию 30000. Не рекомендуется менять, кроме случаев когда HDFS работает крайне нестабильно и наблюдается частая недоступность нод.


### Сортировка файла ###

Записи внутри parquet файла сортируются по значению указанного столбца.<br />
Столбец, по которому требуется производить сортировку, передается параметром метода sort().

```
//Для работы с отдельным файлом нужно создать объект ParquetFile
ParquetFile file = new ParquetFile(new Path(hdfs/path/to/file), configuration);
ParquetSorter sorter = ParquetSorter.builder(file)
                .withRowGroupSize(128 * 1024 *1024)
                .withOutputChunkSize(512 * 1024 * 1024)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .build()
sorter.sort("COLUMN_TO_SORT");
```

### Разбиение файла ###

Крупный файл разбивается на несколько более мелких файлов. В процессе разбияния можно задать опцию сортировки.

```
ParquetSplitter splitter = new ParquetSplitter
                .builder(new ParquetFile(hdfs/path/to/file), configuration)
                .withOutputChunkSize(256 * 1024 *1024)
                .withSorting("COLUMN_TO_SORT")
                .withThreadPoolSize(8)
                .build();
splitter.split();
```

### Конвертация csv в parquet ###

Конвертирует несжатые или упакованные в gzip архив csv файлы в формат parquet.

```
CsvToParquetConverter converter = CsvToParquetConverter.builder()
                                    .withInputPath("/path/to/input/file")
                                    .withOutputPath("path/to/output/file")
                                    .withConf(configuration)
                                    .withDelimiter(';')
                                    .withQuote('"')
                                    .withFirstRecordAsHeader()
                                    .withThreadPoolSize(64)
                                    .withLinesPerThread(500000)
                                    .build();
converter.convert();
```

При создании билдера можно указать следующие опции:
- ```withDelimiter(char delimiter)```: разделитель, используемый в исходном файле. По-умолчанию запятая (,)
- ```withQuote(char quote)```: кавычки, используемые в исходном файле. По-умолчанию знак верхней двойной кавычки (")
- ```withCompressionCodec(CompressionCodecName compressionCodecName)```: кодек для сжатия выходных файлов
- ```withHeader(String[] header)```: список полей файла. Использовать эту опцию, если исходный csv файл не имеет заголовка, или если требуется переименовать поля выходного файла. В этом случае требуется также использовать опцию ```withSkipFirstLine()```
- ```withFirstRecordAsHeader()```: для использования в качестве списка полей первой строки из исходного файла
- ```withSchema(Schema schema)```: использовать готовый объект Schema для формирования выходного parquet файла. Если эта опция не используется, то схема формируется на основе переданного списка полей или первой строки csv файла.
- ```withLinesPerThread(int linesPerThread)```: количество строк, передаваемых в поток конвертации. По-умолчанию 100000
- ```withThreadPoolSize(int threadPoolSize)```: количество одновременно запускаемых потоков конвертации. По умолчанию 64
- ```withInputPath(String inputPath)```: Путь к исходному файлу. Обязательная опция
- ```withOutputPath(String outputPath)```: Путь к выходному файлу. если эта опция не задана, то выходной файл сохраняется по пути (/путь/к/исходному/файлу/converted). Выходной файл бьётся на фрагменты по 128Мб, к имени каждого фрагмента добавляется part + номер фрагмента.
- ```withAllowMissingColumnNames()```: позволяет обрабатывать заголовки с пустыми именами полей. Полезно, если все строки исходного файла оканчиваются разделителями.
- ```withRecordSeparator(String recordSeparator)```: устанавливает определенный разделитель для обрабатываемого файла. Допустимые значения разделителя: '\n', '\r' и "\r\n". Остальные значения будут игнорироваться.
- ```withSkipLinesInBeginning(int linesToSkip)```: позволяет пропустить определенное количество строк в начале файла. Если файл начниается с пустых строк, отсчёт начинается с первой непустой строки.
