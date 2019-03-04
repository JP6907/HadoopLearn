### 倒排索引创建
#### 需求：
有以下文件：           
a.txt 
```$xslt
hello tom
hello jim
hello kitty
hello rose
```
b.txt
```$xslt
hello jerry
hello jim
hello kitty
hello jack
```
c.txt
```$xslt
hello jerry
hello java
hello c++
hello c++    
```
需要得到以下结果：
```$xslt
hello  a.txt-->4  b.txt-->4  c.txt-->4
java   c.txt-->1
jerry  b.txt-->1  c.txt-->1
....
```

#### 思路：
1、先写一个mr程序：统计出每个单词在每个文件中的总次数 
```$xslt
hello-a.txt 4
hello-b.txt 4
hello-c.txt 4
java-c.txt 1
jerry-b.txt 1
jerry-c.txt 1
```
2、然后在写一个mr程序，读取上述结果数据：            
map： 根据-切，以单词做key，后面一段作为value             
reduce： 拼接values里面的每一段，以单词做key，拼接结果做value，输出即可


#### 要点：
map方法中，如何获取所处理的这一行数据所在的文件名？           
worker在调map方法时，会传入一个context，而context中包含了这个worker所读取的数据切片信息，而切片信息又包含这个切片所在的文件信息
那么，就可以在map中：
FileSplit split = context.getInputSplit();
String fileName = split.getpath().getName();

