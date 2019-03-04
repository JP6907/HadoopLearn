package mapreduce.Order.Simple;

import java.util.ArrayList;

/**
 * List里面保存的只是对象的引用
 */
public class KeanTest {

    public static void main(String[] args){

        ArrayList<OrderBean> list = new ArrayList<OrderBean>();

        OrderBean bean = new OrderBean();
        bean.set("1","2","apple",13.3f,2);

        list.add(bean);

        bean.set("2","2","apple",13.3f,2);

        System.out.println(bean);

    }
}
