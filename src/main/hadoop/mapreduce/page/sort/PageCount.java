package mapreduce.page.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageCount implements WritableComparable<PageCount> {

    private String page;
    private int count;

    public PageCount() {
    }

    public PageCount(String page, int count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int compareTo(PageCount o) {
        return o.count-this.count==0?o.page.compareTo(this.page):o.count-this.count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(page);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.page = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.page + "," + this.count;
    }
}
