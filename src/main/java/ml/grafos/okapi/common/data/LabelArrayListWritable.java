package ml.grafos.okapi.common.data;

import de.unipassau.fim.dimis.schlegel.types.WritableLabel;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by schlegel on 19/05/15.
 */
public class LabelArrayListWritable extends ArrayListWritable<WritableLabel> implements WritableComparable<LabelArrayListWritable> {

    /** Default constructor for reflection */
    public LabelArrayListWritable() {
        super();
    }

    @Override
    public void setClass() {
        setClass(WritableLabel.class);
    }

    @Override
    public int compareTo(LabelArrayListWritable message) {

        if (message == null) {
            return 1;
        }
        if (this.size() < message.size()) {
            return -1;
        }
        if (this.size() > message.size()) {
            return 1;
        }

        for (int i=0; i<this.size(); i++) {
            if (this.get(i) == null && message.get(i) == null) {
                continue;
            }
            if (this.get(i)==null) {
                return -1;
            }
            if (message.get(i)==null) {
                return 1;
            }

            return this.get(i).getLabel().compareTo(message.get(i).getLabel());
        }
        return 0;
    }
}
