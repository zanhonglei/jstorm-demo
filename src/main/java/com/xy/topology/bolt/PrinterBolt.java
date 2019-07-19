package com.xy.topology.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;


/**
 * @author zanhonglei
 * @description：
 * @date 2019 2019/7/18 19:47
 * @version:
 * @modified By：
 */
public class PrinterBolt extends BaseBasicBolt {

    private static Logger logger = Logger.getLogger(PrinterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        logger.info(JSON.toJSONString(tuple));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}