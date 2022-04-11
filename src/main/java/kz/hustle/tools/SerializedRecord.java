package kz.hustle.tools;

public class SerializedRecord {
    private String sortableField;
    private byte[] byteArray;

    public SerializedRecord(String sortableField, byte[] byteArray) {
        this.sortableField = sortableField;
        this.byteArray = byteArray;
    }

    public String getSortableField() {
        return sortableField;
    }

    public byte[] getByteArray() {
        return byteArray;
    }

    public void setSortableField(String sortableField) {
        this.sortableField = sortableField;
    }

    public void setByteArray(byte[] byteArray) {
        this.byteArray = byteArray;
    }

    /*@Override
    public int compareTo(Object o) {
        return sortableField.compareTo(((SerializedRecord)o).getSortableField());
    }*/
}
