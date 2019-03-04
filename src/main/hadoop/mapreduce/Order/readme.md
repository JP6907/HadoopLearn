## 分组求TopN

### 数据
```$xslt
编号，用户编号，产品名字，单价，数量
order001,u001,小米6,1999.9,2
order001,u001,雀巢咖啡,99.0,2
order001,u001,安慕希,250.0,2
order001,u001,经典红双喜,200.0,4
order001,u001,防水电脑包,400.0,2
order002,u002,小米手环,199.0,3
order002,u002,榴莲,15.0,10
order002,u002,苹果,4.5,20
order002,u002,肥皂,10.0,40
order003,u001,小米6,1999.9,2
order003,u001,雀巢咖啡,99.0,2
order003,u001,安慕希,250.0,2
order003,u001,经典红双喜,200.0,4
order003,u001,防水电脑包,400.0,2
```
每个订单包含多条数据

### 问题描述
求每个订单中总金额最大的前三条数据




### 方法一
mapper中设置orderid为key，相同orderid的一组数据被分发到一个reduce task        
然后在recude中将数据排序，取前n条



### 方法二
compaereto + Partitioner + GroupComparator             
利用map中的自动排序     
compareto: 优先orderid，再根据总金额            
Partitioner: 按照订单中的orderid来分发数据
GroupComparator ： orderid相同的为一组
