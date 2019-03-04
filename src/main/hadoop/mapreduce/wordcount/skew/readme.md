### 数据倾斜
有一些数据
```aidl
a b a a a a a b c f d e f a f s z a a a b b b b b b r r r r
a b a a a a a b c f d e f a f s z a a a b b b b b b r r r r
a b a a a a a b c f d e f a f s z a a a b b b b b b r r r r
a b a a a a a b c f d e f a f s z a a a b b b b b b r r r r
```
可以看出单词a、b的数量在数据中占大多数，而在map程序中按照单词来分发数据         
这会导致负责a、b的reduce task需要处理大部分数据，而其它reduce task只需要处理小部分数据         
这就是数据倾斜问题，有两种解决方案：
           
#### 方案一Combiner
使用Combiner组件
```aidl
public static class WordCountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable v : values){
                count += v.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

```
Combiner组件能够实现在map task中局部聚合，一个map task处理之后会产生很多k-v的数据          
Combiner可以将同一个map task产生的数据先做一次局部聚合，在分发到reduce task去进行全局聚合
例如：                    
map产生数据：
```aidl
a 1
b 1
c 1
a 1
a 1
b 1
a 1
b 1
```
Combiner处理之后：
```aidl
a 4
b 3
c 1
```
在将以上数据分发到reduce task去处理，这样可以大大减少reduce task的压力，解决数据倾斜问题。


#### 方案二 通用方案：Random打散数据
将map程序产生的key最后都追加一个随机数，随机数的范围为0-reduceNum       
因此能够将同一key的数据平均分配到各个reduce task去处理         
最后再利用一个mr程序去聚合各个reduce task的数据，以及去除key的随机数后缀