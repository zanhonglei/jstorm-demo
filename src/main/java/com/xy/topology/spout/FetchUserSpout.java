package com.xy.topology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.xy.topology.model.UserInfo;
import com.xy.topology.utils.Utils;

import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * @author zanhonglei
 * @description：
 * @date 2019 2019/7/18 19:46
 * @version:
 * @modified By：
 */
public class FetchUserSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    Random _rand;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        _rand = new Random();
    }

    /**
     * 模拟产生用户信息
     */
    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        UserInfo userInfo = new UserInfo();
        userInfo.setAge(String.valueOf(_rand.nextInt()));
        userInfo.setName(Utils.getRandomString(5));
        userInfo.setId(String.valueOf(_rand.nextInt()));
        collector.emit(new Values(userInfo,new Date()));

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //第一个key是 users 依次类推
        declarer.declare(new Fields("users","time"));
    }
}