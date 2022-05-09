package com.dcx.flow;

/**
 * Created by 党楚翔 on 2022/5/9.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        System.out.println("line="+data);
        //1	13736230513	192.196.100.1 www.atguigu.com 2481 24681 200
        String[] s = data.split("\t");
        System.out.println("手机号："+s[1]);
        phone.set(s[1]);
        long upFlow = Long.parseLong(s[s.length - 3]);
        long downFlow = Long.parseLong(s[s.length - 2]);
        flow.set(upFlow,downFlow);
        context.write(phone,flow);
    }
}