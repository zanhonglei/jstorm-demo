package com.xy.topology.local;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.xy.topology.bolt.UserModifyBolt;
import com.xy.topology.spout.FetchUserSpout;
import com.xy.topology.spout.RandomIntegerSpout;
import org.apache.log4j.Logger;

/**
 * @author zanhonglei
 * @description：本地调试 启动类
 * @date 2019 2019/7/18 19:18
 * @version:
 * @modified By：
 */
public class LocalStarter {

    private static Logger logger = Logger.getLogger(LocalStarter.class);

    public static void main(String[] args)  {
        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();


        // 拓扑名，数据源，并行度
        builder.setSpout("user-spout-id", new FetchUserSpout(),1);

        builder.setSpout("random-spout-id", new RandomIntegerSpout(), 1);

        // String componentId 发送方的名字, String streamId 接收方的stream的tuple
//        builder.setBolt("user-modify-bolt-id", new UserModifyBolt(),2).localOrShuffleGrouping("user-spout-id");


        // 分组1： allGrouping  两个spot并行 所有都分发
//        builder.setBolt("user-modify-bolt-id", new UserModifyBolt(),2).allGrouping("user-spout-id");

        // 分组2： localOrShuffleGrouping 首先看当前进程有没有task，如果有就走进程内通信（队列），如果没有就走进程间（Netty），其实就是随机往下游去发,不自觉的做到了负载均衡
//        builder.setBolt("user-modify-bolt-id", new UserModifyBolt(),2).localOrShuffleGrouping("user-spout-id");

        // 分组3： fieldsGrouping  其实就是MapReduce里面理解的Shuffle,根据fields求hash来取模
//        builder.setBolt("user-modify-bolt-id", new UserModifyBolt(),2).fieldsGrouping("user-spout-id",new Fields("users"));

        //分组4：  globalGrouping  只往一个里面发,往taskId小的那个里面去发送
        builder.setBolt("user-modify-bolt-id", new UserModifyBolt(),2).globalGrouping("user-spout-id");


//        builder.setBolt("printer-bolt-id", new PrinterBolt()).localOrShuffleGrouping("user-spout-id").localOrShuffleGrouping("random-spout-id");


        Config conf = new Config();
        //建议加上这行，使得每个bolt/spout的并发度都为1
//        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, "1");


        // conf.put(Config.TOPOLOGY_WORKERS, 4);
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(30);

        //提交拓扑
        cluster.submitTopology("data-topology", conf, builder.createTopology());

        // 集群方式
//        StormSubmitter.submitTopology("data-topology", conf, builder.createTopology());

    }

}
