### 分组求TopN
Partitioner               
CompareGrouping

- 在map程序中，以order对象作为key，value设为空,这是为了利用reduce会根据key自动排序的特点
```$xslt
map1             map2
[o1.900]        [o1.600]
[o1.500]        [o2.400]
[o1.300]        [o2.300]
[o2.500]        [o3.600]
``` 

- 第一，让orderid相同的数据发送给相同的reduce task，同时自动排序
    - 修改Partitioner，让orderid相同就返回相同的分区号
    - 通过bean上的compareto方法实现让框架将数据按照oirderid，次之按照订单总金额倒序排序
```$xslt
[o1,900]
[o1,400]
[o1,600]
[o1,200]

[o3,400]
[o3,600]
```
- 第二，让reduce task程序认为orderid相同的key（bean），属于同一组
    - 修改GroupingComparator逻辑
    - 虽然map程序中返回是以对象作为key，value为空，在reduce的迭代器中的值都为空，但是迭代器的每一次迭代都会改变key的值，所以可以通过key取到同一组数据的不同值