有订单数据和用户数据，通过用户id关联，一个用户可能对应多个订单    
订单数据：
```$xslt
order001,u001
order002,u001
order003,u005
order004,u002
order005,u003
order006,u004
```
用户数据：
```$xslt
u001,senge,18,angelababy
u002,laozhao,48,ruhua
u003,xiaoxu,16,chunge
u004,laoyang,28,zengge
u005,nana,14,huangbo
```
求：将两个表中的同一份数据组合起来：
```$xslt
order001,u001,senge,18,angelababy
order002,u001,senge,18,angelababy
...
```

最简单的思路在map程序中将userid作为key输出，在reduce中将数据做组合


还可以利用Partitioner+CompareTo+GroupingComparator 来高效实现         
实现迭代器中第一条数据是user，后面的全部是order，就不需要将全部数据在内存中缓存

改变排序，考虑userid（默认）、表名（user表的数据放前面）          
添加表名，会影响分发，需改写分发规则，还要控制分组策略（userid相同则为一组）




### Partitioner+CompareTo+GroupingComparator
在map中的自动排序是利用key的大小进行排序，要想利用map的自动排序，就需要把bean作为key传输，
我们希望的排序规则是同一个userid的排在一起，并且用户表在前面，后面紧接对应的所有订单表       
所以需要修改compaerto函数，优先根据userid，再根据tableName          
使用bean作为key，则会影响分发规则，必须构造Partitioner子类，将userid相同的发送给同一个reduce task         
还需要构造GroupingComparator子类，使userid相同的作为一组

