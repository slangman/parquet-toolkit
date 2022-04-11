package kz.hustle.tools;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.util.Collection;
import java.util.Iterator;

public class DMCGenericData extends GenericData {

    private String sortField;

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public static class Record extends GenericData.Record {

        public Record(Schema schema) {
            super(schema);
        }

        @Override
        public int compareTo(GenericData.Record that) {
            return DMCGenericData.get().compare(this, that, super.getSchema());
        }

    }

    @Override
    public int compare(Object o1, Object o2, Schema s) {
        return compare(o1, o2, s, false);
    }

    @Override
    protected int compare(Object o1, Object o2, Schema s, boolean equals) {
        if (o1 == o2)
            return 0;
        switch (s.getType()) {
            case RECORD:
                Schema.Field f = s.getField(sortField);
                int pos = f.pos();
                String name = f.name();
                int compare1 = compare(getField(o1, name, pos), getField(o2, name, pos), f.schema(), equals);
                if (compare1 != 0) // not equal
                    return f.order() == Schema.Field.Order.DESCENDING ? -compare1 : compare1;
                return 0;
            case ENUM:
                return s.getEnumOrdinal(o1.toString()) - s.getEnumOrdinal(o2.toString());
            case ARRAY:
                Collection a1 = (Collection) o1;
                Collection a2 = (Collection) o2;
                Iterator e1 = a1.iterator();
                Iterator e2 = a2.iterator();
                Schema elementType = s.getElementType();
                while (e1.hasNext() && e2.hasNext()) {
                    int compare = compare(e1.next(), e2.next(), elementType, equals);
                    if (compare != 0)
                        return compare;
                }
                return e1.hasNext() ? 1 : (e2.hasNext() ? -1 : 0);
            case MAP:
                if (equals)
                    return o1.equals(o2) ? 0 : 1;
                throw new AvroRuntimeException("Can't compare maps!");
            case UNION:
                int i1 = resolveUnion(s, o1);
                int i2 = resolveUnion(s, o2);
                return (i1 == i2) ? compare(o1, o2, s.getTypes().get(i1), equals) : Integer.compare(i1, i2);
            case NULL:
                return 0;
            case STRING:
                Utf8 u1 = o1 instanceof Utf8 ? (Utf8) o1 : new Utf8(o1.toString());
                Utf8 u2 = o2 instanceof Utf8 ? (Utf8) o2 : new Utf8(o2.toString());
                return u1.compareTo(u2);
            default:
                return ((Comparable) o1).compareTo(o2);
        }
    }

}
