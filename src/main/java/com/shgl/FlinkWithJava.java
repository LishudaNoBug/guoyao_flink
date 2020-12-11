package com.shgl;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * 用的hashMap判断是否有key，kafka数据类型是分号分隔的字符串。第二版项目。
 * 现在改成第四版了？  输出到本地的目录与HDFS完全相同，根据 mmsi+年月日 自动滚动。后期用shell脚本自动put到hdfs上。
 */

public class FlinkWithJava {

    //  /usr/local/flink-1.10.0/jobJar/jobResult/
    //  E:/bufferWriter/
    public static final String pathName = "/usr/local/flink-1.10.0/jobJar/jobResult/";

    public static final SimpleDateFormat yearMonthDay = new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat yearMonth = new SimpleDateFormat("yyyyMM");


    //对已存在文件追加字符串
    public static void fileAppend(String file, String conent) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            out.write(conent);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.240:9092");
        properties.setProperty("group.id", "sadll");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource kafkaStream = env.addSource(new FlinkKafkaConsumer011("testTopic", new SimpleStringSchema(), properties));
        kafkaStream.print();
        //++++++++++++++++++++++++++++++++++++上面从kafka获取数据没问题




        DataStream<ShipEntity> windowCount = kafkaStream.flatMap(new RichFlatMapFunction<String, ShipEntity>() {

            public void flatMap(String in, Collector<ShipEntity> out) throws Exception {
                /*
                封装成实体类
                 */
                String[] split = in.split(";");
                ShipEntity shipEntity=new ShipEntity(split[0],split[1],split[2],split[3],split[4],split[5],split[6],split[7],split[8],split[9],split[10],split[11],split[12],split[13]);
                //将destination_tidied字段中逗号替换为 &
                shipEntity.destination_tidied=shipEntity.destination_tidied.replace(",", " & ");

                /*
                拿到当前时间生成目录和文件用
                 */
                Date date=new Date();
                String yearMonthDayString=yearMonthDay.format(date);
                String yearMonthString=yearMonth.format(date);
                //创建 /mmsi=23500012/month=202008  格式的目录,  00:00后会自动更新
                File file = new File(pathName+"mmsi="+shipEntity.mmsi+"/"+"month="+yearMonthString);
                if(!file.exists() || !file.isDirectory()) {
                    file.mkdirs();
                }
                //判断是否有 /mmis/年月/年月日.txt  命名的文件，没有则创建，有则直接写入
                File txtFile = new File(file+"/"+yearMonthDayString+".txt");
                if (txtFile.exists()){
                    fileAppend(txtFile.toString(),shipEntity.toString());
                    System.out.println("有直接写入");
                }else if (!txtFile.exists()){
                    BufferedWriter bw = new BufferedWriter(new FileWriter(txtFile.toString())) ;
                    bw.write(shipEntity.toString());
                    bw.flush();
                    bw.close();
                    System.out.println("没有创建");
                }

            }
        });

        env.execute("kafka to txt by flink_java");
    }
}
