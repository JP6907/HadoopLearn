package mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        int upFlow = Integer.parseInt(fields[fields.length-3]);
        int downFlow = Integer.parseInt(fields[fields.length-2]);
        String phone = fields[1];

        FlowBean flowBean = new FlowBean(phone,upFlow,downFlow);
        context.write(new Text(phone), flowBean);
    }
}
