package mapreduce.page.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageTopnReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    /**
     * 这种方法只是计算出每个reduce进程的topn，而不是全局的topn
     * 如果需要全局，则只能设置一个reduce
     * 而且所有数据都必须保存在内存中,只适合小数据量
     */
    TreeMap<PageCount,Object> treeMap = new TreeMap<PageCount, Object>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }
        PageCount pageCount = new PageCount(key.toString(),count);
        treeMap.put(pageCount,null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        int topn = conf.getInt("topn",5);

        Set<Map.Entry<PageCount, Object>> entrySet = treeMap.entrySet();
        int i = 0;
        for(Map.Entry<PageCount,Object> set : entrySet){
            context.write(new Text(set.getKey().getPage()),new IntWritable(set.getKey().getCount()));
            if(++i==topn)
                return;
        }
    }
}
