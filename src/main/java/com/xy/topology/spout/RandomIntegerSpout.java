package com.xy.topology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.xy.topology.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @author zanhonglei
 * @description：消息消费者 Spout
 * @date 2019 2019/7/18 18:57
 * @version:
 * @modified By：
 */
public class RandomIntegerSpout implements IRichSpout {

    private static final Logger LOG   = LoggerFactory.getLogger(RandomIntegerSpout.class);

    private SpoutOutputCollector collector;
    private Random rand;
    private long msgId = 0;

    /**
     * spout可以有构造函数，但构造函数只执行一次，是在提交任务时，创建spout对象，
     * 因此在task分配到具体worker之前的初始化工作可以在此处完成，一旦完成，初始化的内容将携带到每一个task内
     * （因为提交任务时将spout序列化到文件中去，在worker起来时再将spout从文件中反序列化出来）。
     */
    public RandomIntegerSpout(){

    }

    /**
     * open是当task起来后执行的初始化动作
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.rand = new Random();
    }

    /**
     * close是当task被shutdown后执行的动作
     */
    @Override
    public void close() {

    }

    /**
     * activate 是当task被激活时，触发的动作
     */
    @Override
    public void activate() {

    }

    /**
     * deactivate 是task被deactive时，触发的动作
     */
    @Override
    public void deactivate() {

    }

    /**
     * nextTuple 是spout实现核心， nextuple完成自己的逻辑，即每一次取消息后，用collector 将消息emit出去。
     */
    @Override
    public void nextTuple() {
        Utils.sleep(100);
        collector.emit("random-stream",new Values(rand.nextInt(1000), System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId),
                msgId);
    }

    /**
     * ack， 当spout收到一条ack消息时，触发的动作，详情可以参考 ack机制
     * @param o
     */
    @Override
    public void ack(Object o) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    /**
     * fail， 当spout收到一条fail消息时，触发的动作，详情可以参考 ack机制
     * @param o
     */
    @Override
    public void fail(Object o) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }

    /**
     * declareOutputFields， 定义spout发送数据，每个字段的含义
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value", "ts", "msgid"));

    }

    /**
     * getComponentConfiguration 获取本spout的component 配置
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {

        return null;
    }
}
