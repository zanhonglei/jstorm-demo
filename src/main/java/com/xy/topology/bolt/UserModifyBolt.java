package com.xy.topology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.xy.topology.model.UserInfo;
import com.xy.topology.utils.Utils;
import org.apache.log4j.Logger;
import shade.storm.org.json.simple.JSONValue;

import java.util.Map;

/**
 * @author zanhonglei
 * @description：数据库Bolt
 * @date 2019 2019/7/18 19:05
 * @version:
 * @modified By：
 */
public class UserModifyBolt implements IRichBolt {

    private static Logger logger = Logger.getLogger(UserModifyBolt.class);

    private OutputCollector collector;

    /**
     * bolt可以有构造函数，但构造函数只执行一次，是在提交任务时，创建bolt对象，
     * 因此在task分配到具体worker之前的初始化工作可以在此处完成，一旦完成，初始化的内容将携带到每一个task内
     * （因为提交任务时将bolt序列化到文件中去，在worker起来时再将bolt从文件中反序列化出来）。
     */
    public UserModifyBolt() {

    }

    /**
     * prepare是当task起来后执行的初始化动作
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    /**
     * execute是bolt实现核心， 完成自己的逻辑，即接受每一次取消息后，处理完，
     * 有可能用collector 将产生的新消息emit出去。
     * 在executor中，当程序处理一条消息时，需要执行collector.ack， 详情可以参考 ack机制
     * 在executor中，当程序无法处理一条消息时或出错时，需要执行collector.fail ，详情可以参考 ack机制
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        Fields fields = input.getFields();
        String userInfoBuffer = fields.get(0);
        logger.info(userInfoBuffer);
        UserInfo userInfo = (UserInfo) JSON.parse(userInfoBuffer);
        userInfo.setAge("11111111");
        userInfo.setName(Utils.getRandomString(5));
        userInfo.setId("22222222");
        collector.emit(new Values(userInfo));
    }

    /**
     * cleanup是当task被shutdown后执行的动作
     */
    @Override
    public void cleanup() {

    }

    /**
     * declareOutputFields， 定义bolt发送数据，每个字段的含义
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * getComponentConfiguration 获取本bolt的component 配置
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
