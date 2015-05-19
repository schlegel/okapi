package de.unipassau.fim.dimis.schlegel.types;

import de.unipassau.fim.dimis.schlegel.exceptions.GiraphAndBalloonsException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * A writable label similar to {@link org.apache.hadoop.io.Text} but with less overhead.
 * It is simple enough to rely on the Java String encode/decode methods and does not utilize
 * hadoop's encoder factory.
 */
public class WritableLabel extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    /**
     * Creates a new empty label.
     */
    public WritableLabel() {
        this.setLabel(EMPTY_BYTES);
    }

    /**
     * Shallow clone constructor.
     *
     * @param original the original to clone
     */
    public WritableLabel(WritableLabel original) {
        this(original.getLabel());
    }

    /**
     * Creates a new label with the string given.
     *
     * @param string the label
     */
    public WritableLabel(String string) {
        this.setLabel(string);
    }

    /**
     * Creates a new label from the bytes given.
     *
     * @param bytes the label's bytes
     */
    public WritableLabel(byte[] bytes) {
        this.setLabel(bytes);
    }

    /**
     * Set the label's string.
     *
     * @param string the label's string
     */
    public void setLabel(String string) {
        try {
            this.setLabel(string.getBytes(DEFAULT_CHARSET));
        } catch (UnsupportedEncodingException ex) {
            throw new GiraphAndBalloonsException(DEFAULT_CHARSET
                    + " encoding is unsupported.", ex);
        }
    }

    /**
     * Set the label's bytes.
     *
     * @param bytes the label's bytes
     */
    public void setLabel(byte[] bytes) {
        this.setBytes(bytes, 0, bytes.length);
    }

    /**
     * Utility method set offset and length of the byte array to set.
     *
     * @param bytes the bytes to set
     * @param start the offset
     * @param length the length to copy
     */
    private void setBytes(byte[] bytes, int start, int length) {
        this.setCapacity(length);
        System.arraycopy(bytes, start, this.bytes, 0, length);
        this.length = length;
    }

    /**
     * Set the byte capacity to at least the given length.
     *
     * @param length the new capacity
     */
    private void setCapacity(int length) {
        if ((this.bytes == null) || (this.bytes.length < length)) {
            this.bytes = new byte[length];
        }
    }

    /**
     * Returns the label's byte length.
     *
     * @return the length of the label's bytes.
     */
    @Override
    public int getLength() {
        return this.length;
    }

    /**
     * Returns the label's bytes.
     *
     * @return the label's bytes
     */
    @Override
    public byte[] getBytes() {
        return this.bytes;
    }

    /**
     * Retrieves the label's string.
     *
     * @return the label
     */
    public String getLabel() {
        try {
            return new String(this.bytes, 0, this.length, DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException ex) {
            throw new GiraphAndBalloonsException(DEFAULT_CHARSET
                    + " encoding is unsupported.", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String   toString() {
        return this.getLabel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Serializes the label's bytes.
     *
     * @param dataOutput the data output to write to
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVInt(dataOutput, this.length);
        dataOutput.write(this.bytes, 0, this.length);
    }

    /**
     * Reads a serialized representation of the label.
     *
     * @param dataInput the data input to read from
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.length = WritableUtils.readVInt(dataInput);
        this.bytes = new byte[this.length];
        dataInput.readFully(this.bytes);
    }

    /**
     * A static factory method.
     *
     * @param dataInput the data input to read from
     * @return a new instance
     * @throws IOException
     */
    public static WritableLabel read(DataInput dataInput)
            throws IOException {
        WritableLabel writableLabel = new WritableLabel();
        writableLabel.readFields(dataInput);
        return writableLabel;
    }
}
