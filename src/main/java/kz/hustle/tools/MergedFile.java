package kz.hustle.tools;

public class MergedFile {
    private String path;
    private String fileName;
    private boolean mustBeDeleted;
    private long length;

    public MergedFile() {
    }

    public MergedFile(String path, String fileName, boolean mustBeDeleted) {
        this.path = path;
        this.fileName = fileName;
        this.mustBeDeleted = mustBeDeleted;
    }

    public MergedFile(String path, String fileName, long length, boolean mustBeDeleted) {
        this.path = path;
        this.fileName = fileName;
        this.length = length;
        this.mustBeDeleted = mustBeDeleted;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean mustBeDeleted() {
        return mustBeDeleted;
    }

    public void setMustBeDeleted(boolean mustBeDeleted) {
        this.mustBeDeleted = mustBeDeleted;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
