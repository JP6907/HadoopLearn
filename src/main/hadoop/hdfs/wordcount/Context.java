package hdfs.wordcount;

import java.util.HashMap;

public class Context {
    private HashMap<Object,Object> map = new HashMap<Object,Object>();

    public void write(Object key,Object value){
        map.put(key,value);
    }

    public Object get(Object key){
        return map.get(key);
    }

    public HashMap<Object,Object> getContextMap(){
        return map;
    }
}
