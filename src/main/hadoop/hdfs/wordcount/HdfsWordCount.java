package hdfs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordCount {

    public static void main(String[] args) throws Exception{
        //初始化配置
        Properties pros = new Properties();
        pros.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));

        //利用反射动态加载Mapper具体实现类
        Class<?> mapper_class = Class.forName(pros.getProperty(Constants.MAPPER_CLASS));
        Mapper mapper = (Mapper) mapper_class.newInstance();

        Context context = new Context();

        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"),new Configuration(),"root");
        Path inPath = new Path(pros.getProperty(Constants.INPUT_PATH));
        Path outPath = new Path(pros.getProperty(Constants.OUTPUT_PATH));

        if(!fs.exists(inPath)){
            throw new RuntimeException("指定输入目录不存在！");
        }
        if(fs.exists(outPath)){
            throw new RuntimeException("指定输出目录已存在，请更换！");
        }

        //输入目录下的每个文件逐行读取数据
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(inPath, false);
        while(itr.hasNext()){
            LocatedFileStatus file = itr.next();
            //文件的每一行
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while((line=br.readLine())!=null){
                //对行进行业务处理
                mapper.map(line,context);
            }
            in.close();
            br.close();
        }

        //输出结果
        FSDataOutputStream outputStream = fs.create(outPath);

        HashMap<Object,Object> map = context.getContextMap();
        Set<Map.Entry<Object, Object>> entrySet = map.entrySet();
        for(Map.Entry<Object,Object> entry : entrySet){
            outputStream.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }

        outputStream.close();
        fs.close();
        System.out.println("数据统计完成！");
    }

}
