package mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 按照号码归属地来控制map的数据分发
 * MapTask通过这个类的getPartition方法，来计算它所产生的每一对kv数据该分发给哪一个reduce task
 * @param <Text>
 * @param <FlowBean>
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    static HashMap<String,Integer> codeMap = new HashMap<String, Integer>();

    static {
        codeMap.put("135",0);
        codeMap.put("136",1);
        codeMap.put("137",2);
        codeMap.put("138",3);
    }

    public int getPartition(Text text, FlowBean flowBean, int i) {

        Integer res = codeMap.get(text.toString().substring(0,3));
        return res==null?4:res;
    }
}
