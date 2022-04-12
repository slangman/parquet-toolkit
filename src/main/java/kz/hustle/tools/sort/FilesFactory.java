package kz.hustle.tools.sort;

//import kz.dmc.packages.threads.pools.DMCThreadsPool;
import org.apache.hadoop.fs.FileStatus;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

public class FilesFactory {
    private int parallelFilesProcessing = 0;
    private long partFileSize = 0;
    private FileStatus[] files = null;
    private FileProcessor fileProcessor;
    private SortThreadPool processorsThreadPool = null;
    private SortThreadPool partFilePool = null;

    public FilesFactory(FileProcessor fileProcessor) {
        this.fileProcessor = fileProcessor;
    }

    public FilesFactory setParallelFilesProcessing(int parallelFilesProcessing) {
        this.parallelFilesProcessing = parallelFilesProcessing;
        return this;
    }

    public FilesFactory setPartFileSize(long partFileSize) throws Exception {
        this.partFileSize = partFileSize;
        if (this.partFileSize != 0 && isOverriden("setPartFileSize")) {
            fileProcessor.setPartFileSize(partFileSize);
        }
        return this;
    }

    public FilesFactory setFiles(FileStatus[] files) {
        this.files = files;
        return this;
    }

    public FilesFactory setProcessorsThreadPool(SortThreadPool processorsThreadPool) throws Exception {
        this.processorsThreadPool = processorsThreadPool;
        if (this.processorsThreadPool != null && isOverriden("setFactoryPool")) {
            fileProcessor.setFactoryPool(this.processorsThreadPool);
        }

        return this;
    }

    public FilesFactory setPartFilePool(SortThreadPool partFilePool) throws Exception {
        this.partFilePool = partFilePool;
        if (this.partFilePool != null && isOverriden("setPartFilePool")) {
            fileProcessor.setPartFilePool(this.partFilePool);
        }

        return this;
    }

    public void factoryLaunch() throws Exception {
        if (files == null) {
            throw new Exception("Необходимо установить массив с файлами 'setFiles(FileStatus[] files)'");
        } else if (processorsThreadPool == null) {
            throw new Exception("Необходимо установить пул потоков для обработки файлов 'setFactoryPool(DMCThreadsPool factoryPool)'");
        }

        for (FileStatus file : files) {

            fileProcessor.execute(file);

            if (processorsThreadPool != null) {
                while (processorsThreadPool.getActiveTaskCount() >= (parallelFilesProcessing + 10)) {
                    Thread.sleep(500);
                }
            }
        }

        if (processorsThreadPool != null) processorsThreadPool.shutDownAndWait();
        if (partFilePool != null) partFilePool.shutDownAndWait();

        //DMCProgressBar.get().close();

    }

    private boolean isOverriden(String methodName) throws Exception {
        boolean _result = false;
        Optional<Method> m = Arrays.stream(
                FileProcessor.class
                        .getDeclaredMethods())
                .filter(f -> f.getName().equals(methodName))
                .findFirst();


        if (fileProcessor.getClass().getSimpleName().equals(fileProcessor.getClass().getMethod(m.get().getName(), m.get().getParameterTypes()).getDeclaringClass().getSimpleName())) {
            _result = true;
        }

        return _result;
    }

}
