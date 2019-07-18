package com.xy.topology.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author zanhonglei
 * @description：
 * @date 2019 2019/7/18 19:47
 * @version:
 * @modified By：
 */
public class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}