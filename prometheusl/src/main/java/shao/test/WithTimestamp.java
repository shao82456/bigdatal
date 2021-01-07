package shao.test;

/**
 * Author: shaoff
 * Date: 2020/4/28 19:55
 * Package: shao.test
 * Description:
 * 1.prometheus直接刮数据，无法使用自定义时间戳
 * 2.使用pushgateway推数据，也无法设置时间戳
 * https://github.com/prometheus/pushgateway
 * 3.将时间戳放入某个标签存储，性能不好，时间戳的值没有上限
 * https://prometheus.io/docs/practices/naming/#labels
 */
public class WithTimestamp {
//    public stat
    public static void main(String[] args) {
        System.out.println("...");
    }
}
