###问题描述
A:B,C,D,F,E,O        
B:A,C,E,K          
C:F,A,D,I          
D:A,E,F,L             
E:B,C,D,M,L          
F:A,B,C,D,E,O,M          
G:A,C,D,E,F          
H:A,C,D,E,O          
I:A,O           
J:B,O           
K:A,C,D        
L:D,E,F         
M:E,F,G       
O:A,H,I,J         

求：哪些用户两两之间有共同好友，及共同好友都是哪些人:      
A-B  C,E
A-C  D,F

### 思路
在一个人的所有好友中，两两之间的共同好友就有这个人          
A:B,C,D,F表示A的好友有B,C,D,F，也就是B,C,D,F是A的好友        
#### 第一步：
求哪些人两两之间有哪个共同好友          
mapper：
读一行：A:B,C,D 分解成：   
 <B,A> <C,A> <D,A>...      
reducer将key相同的聚集在一起，可以得到：         

<C,A><C,B><C,E><C,F><C,G>…               
输出:        
Key:C      
value: [ A, B, E, F, G ]          
表示：C是这些用户的共同好友

#### 第二步：             
由第一个mr程度可以得到每个用户分别是那些用户的共同好友，也就是这些用户两两之间的共同好友           
mapper：          
两两配对，得到：          
A-B C                     
reduce：             
聚集所有的用户对的共同好友         
A-B： C D E
            

mapper：           
读取一行可以获得的信息为谁和谁是朋友，比如A:B,C,D,F,E,O ：       
A-B,A-C,A-D,A-E,A-F,A-O                    
reducer：           
A-B,A-C,B-C则
A-B:C        
A-C:B          
B-C:A          


