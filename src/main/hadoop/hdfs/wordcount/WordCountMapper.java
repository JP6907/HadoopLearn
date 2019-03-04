package hdfs.wordcount;

public class WordCountMapper implements Mapper{

    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for (String word : words){
            Object value = context.get(word);
            if(value!=null){
                int count = (Integer)value;
                context.write(word,count+1);
            }else{
                context.write(word,1);
            }
        }
    }
}
