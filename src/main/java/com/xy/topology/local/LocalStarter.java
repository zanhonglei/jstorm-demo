package com.xy.topology.local;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.xy.topology.bolt.PrinterBolt;
import com.xy.topology.bolt.UserModifyBolt;
import com.xy.topology.spout.FetchUserSpout;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

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

//        builder.setSpout("random-spout-id", new RandomIntegerSpout(),1);

        builder.setSpout("user-spout-id", new FetchUserSpout(),1);

        // String componentId 发送方的名字, String streamId 接收方的stream的tuple
        builder.setBolt("user-modify-bolt-id", new UserModifyBolt()).localOrShuffleGrouping("user-spout-id");

        builder.setBolt("printer-bolt-id", new PrinterBolt()).localOrShuffleGrouping("user-spout-id");

        Map<String, String> conf = new HashMap<>();

        //建议加上这行，使得每个bolt/spout的并发度都为1
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, "1");

        //提交拓扑
        cluster.submitTopology("data-topology", conf, builder.createTopology());


    }

}
