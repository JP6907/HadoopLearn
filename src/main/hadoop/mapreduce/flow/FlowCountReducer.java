package mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text phone, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upSum = 0;
        int downSum = 0;
        for(FlowBean flow : values){
            upSum += flow.getUpFlow();
            downSum += flow.getDownFlow();
        }
        context.write(phone,new FlowBean(phone.toString(),upSum,downSum));
    }
}
