package hdfs.wordcount;

/**
 * 对行进行业务处理的接口
 */
public interface Mapper {

    public void map(String line, Context context);
}
