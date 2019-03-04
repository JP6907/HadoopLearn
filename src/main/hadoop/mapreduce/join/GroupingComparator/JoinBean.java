package mapreduce.join.GroupingComparator;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements WritableComparable<JoinBean> {

    private String orderId;
    private String userId;
    private String userName;
    private int userAge;
    private String userFriend;
    private String tableName;

    public void set(String orderId, String userId, String userName, int userAge, String userFriend, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.userName = userName;
        this.userAge = userAge;
        this.userFriend = userFriend;
        this.tableName = tableName;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getUserAge() {
        return userAge;
    }

    public void setUserAge(int userAge) {
        this.userAge = userAge;
    }

    public String getUserFriend() {
        return userFriend;
    }

    public void setUserFriend(String userFriend) {
        this.userFriend = userFriend;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return this.orderId + "," + this.userId + "," + this.userAge + "," + this.userName + "," + this.userFriend;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.userId);
        out.writeUTF(this.userName);
        out.writeInt(this.userAge);
        out.writeUTF(this.userFriend);
        out.writeUTF(this.tableName);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.userName = in.readUTF();
        this.userAge = in.readInt();
        this.userFriend = in.readUTF();
        this.tableName = in.readUTF();
    }

    public int compareTo(JoinBean o) {
        //user表在order表前面，user应该作为小值
        return this.userId.compareTo(o.getUserId())==0?o.getTableName().compareTo(this.getTableName()):this.userId.compareTo(o.getUserId());
    }
}
