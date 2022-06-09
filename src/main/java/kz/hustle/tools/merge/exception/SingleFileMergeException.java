package kz.hustle.tools.merge.exception;

public class SingleFileMergeException extends Exception{
    public SingleFileMergeException(String path) {
        super("Can not launch merger. The input path " + path + " is a single file.");
    }
}
