数据格式：
```$xslt
1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
1363157995052 	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4			4	0	264	0	200
1363157991076 	13926435656	20-10-7A-28-CC-0A:CMCC	120.196.100.99			2	4	132	1512	200
```

统计每个用户的上传流量、下载流量、总流量               
并根据省份（手机号）写如到不同的文件中               


思路：构造ProvincePartitioner，根据手机号将数据分发到对应的reduce task