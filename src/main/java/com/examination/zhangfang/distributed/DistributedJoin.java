package com.examination.zhangfang.distributed;

import com.examination.zhangfang.distributed.constants.Constant;
import com.examination.zhangfang.distributed.enums.BusinessEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 3、分布式计算
 *    两批数据集，一批是(userId,regDate)，一批是(transId,userId,amount,transDate)交易流水记录，均分布存储在100台计算器中，使用何种方式可以统计用户的交易次数：(userId,regDate,transCount)
 *    已知userId的购买商品的记录中，有少量特殊userId，这些userId有大量的购买记录（单机无法承受单个用户的所有购买记录）。如何处理？
 */
public class DistributedJoin {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedJoin.class);

    //定义用户文件地址列表
    public static final List<String> userPathList = new ArrayList<>(256);
    //定义交易文件地址列表
    public static final List<String> tradePathList = new ArrayList<>(256);
    //定义将本地用户文件合并上传到HDFS的路径
    public static final String userHdfsPath = Constant.HADOOPURI + "/zhangfang/user_001.txt";
    //定义将本地交易文件合并上传到HDFS的路径
    public static final String tradeHdfsPath = Constant.HADOOPURI + "/zhangfang/trade_001.txt";
    //定义将用户和交易文件关联计算后的结果在HDFS的临时分区路径
    public static final String joinTempHdfsPath = Constant.HADOOPURI + "/zhangfang/joinTemp_001";
    //定义将用户和交易文件关联计算后的结果在HDFS的join路径
    public static final String joinHdfsPath = Constant.HADOOPURI + "/zhangfang/join_001";


    /**
     * MAIN
     */
    public static void main(String[] args) {
        try {
            //预处理，加载用户和交易文件地址
            String rootPath = DistributedJoin.class.getResource("/").getFile();
            //初始化用户文件列表（userId,regDate），如：zhangfang,2012-09-09
            userPathList.add(rootPath + "distributed/user001.txt");
            userPathList.add(rootPath + "distributed/user002.txt");
            //初始化交易文件列表(transId,userId,amount,transDate)，如：001,zhangfang,30.00,2018-12-12
            tradePathList.add(rootPath + "distributed/trade001.txt");
            tradePathList.add(rootPath + "distributed/trade002.txt");


            //初始化Hadoop配置
            Configuration conf = new Configuration();
            //对用户文件进行合并上传到HDFS
            mergeSmallFilesToHDFS(conf, userPathList, userHdfsPath);
            //对交易文件进行合并上传到HDFS
            mergeSmallFilesToHDFS(conf, tradePathList, tradeHdfsPath);
            //第一个job处理（将join结果随机分配到hdfs，解决热点问题）;
            firstJobDeal(conf);
            //汇总随机分配到hdfs的临时文件进行二次处理，保证相同的key在同一个分片上。
            secondJobDeal(conf);

        } catch (Exception e) {
            LOGGER.error("{}", e.getMessage(), e);
        }
    }

    /**
     * 将join结果随机分配到hdfs，解决热点问题
     *
     * @param conf
     * @throws Exception
     */
    public static void firstJobDeal(Configuration conf) throws Exception {
        //定义Hadoop firstjob
        Job firstjob = Job.getInstance();
        firstjob.setJarByClass(DistributedJoin.class);
        //设置Hadoop多文件输入处理的Mapper（多个Map）
        MultipleInputs.addInputPath(firstjob, new Path(userHdfsPath),
                TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(firstjob, new Path(tradeHdfsPath),
                TextInputFormat.class, TradeMapper.class);
        // 指定<k2,v2>的类型
        firstjob.setMapOutputKeyClass(Text.class);
        firstjob.setMapOutputValueClass(Text.class);
        // 指定<k3,v3>的类型
        firstjob.setOutputKeyClass(Text.class);
        firstjob.setOutputKeyClass(Text.class);

        //设置Hadoop多文件输入Reducer
        firstjob.setReducerClass(JoinReducer.class);
        //设置Hadoop分区处理
        //可配置
        firstjob.setNumReduceTasks(10);
        firstjob.setPartitionerClass(JoinPartitioner.class);

        //设置将用户和交易文件关联计算后Reducer的输出路径(此时该路径下的key会存在与不同分区，所以需要第二个job对数据进行处理)
        Path joinTempHdfs = new Path(joinTempHdfsPath);
        FileSystem fileSystem = FileSystem.get(new URI(Constant.HADOOPURI), conf);
        if (fileSystem.exists(joinTempHdfs)) {
            fileSystem.delete(joinTempHdfs, true);
        }
        FileOutputFormat.setOutputPath(firstjob, joinTempHdfs);
        LOGGER.info("{}", firstjob.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 汇总随机分配到hdfs的临时文件进行二次处理，保证相同的key在同一个分片上。
     *
     * @param conf
     * @throws Exception
     */
    public static void secondJobDeal(Configuration conf) throws Exception {
        //定义Hadoop firstjob
        Job secondJob = Job.getInstance();
        secondJob.setJarByClass(DistributedJoin.class);
        //通配符获取临时文件列表
        FileSystem fileSystem = FileSystem.get(new URI(Constant.HADOOPURI), conf);
        FileStatus[] status = fileSystem.globStatus(new Path(joinTempHdfsPath + "/part-r-*"));
        for (FileStatus f : status) {
            MultipleInputs.addInputPath(secondJob, f.getPath(),
                    TextInputFormat.class, SummaryMapper.class);
        }
        // 指定<k2,v2>的类型
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);
        // 指定<k3,v3>的类型
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputKeyClass(Text.class);
        //设置Hadoop多文件输入Reducer
        secondJob.setReducerClass(SummaryReducer.class);
        Path joinHdfs = new Path(joinHdfsPath);
        if (fileSystem.exists(joinHdfs)) {
            fileSystem.delete(joinHdfs, true);
        }
        FileOutputFormat.setOutputPath(secondJob, joinHdfs);
        LOGGER.info("{}", secondJob.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * INIT
     * 将小文件合并上传到HDFS
     *
     * @param conf
     * @param filePathList
     * @param hdfsPath
     * @throws Exception
     */
    public static void mergeSmallFilesToHDFS(Configuration conf, List<String> filePathList, String hdfsPath) throws Exception {
        //定义本地文件系统对象
        FileSystem local = null;
        //定义HDFS上的文件系统对象
        FileSystem hdfs = null;
        //设置文件系统访问接口，并创建FileSystem在本地的运行模式
        URI uri = new URI(Constant.HADOOPURI);
        hdfs = FileSystem.get(uri, conf);
        //获取本地文件系统
        local = FileSystem.getLocal(conf);
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        // Hdfs输出路径
        Path outBlock = new Path(hdfsPath);
        // 打开输出流
        out = hdfs.create(outBlock);
        //循环操作所有文件并复制到HDFS
        for (String filePath : filePathList) {
            Path path = new Path(filePath);
            in = local.open(path);// 打开输入流
            IOUtils.copyBytes(in, out, 4096, false);// 复制数据
            in.close();// 关闭输入流
        }
        if (out != null) {
            out.close();// 关闭输出流
        }
    }

    /**
     * Map
     * 处理交易文本的Mapper
     */
    public static class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() > 0) {
                String[] str = line.split(",");
                context.write(new Text(str[1].trim()),
                        new Text(str[0] + ":" + BusinessEnum.TRADE.getCode()));
            }
        }
    }


    /**
     * Map
     * 处理用户文本的Mapper
     */
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() > 0) {
                String[] str = line.split(",");
                context.write(new Text(str[0].trim()),
                        new Text(str[1] + ":" + BusinessEnum.USER.getCode()));
            }
        }
    }

    /**
     * Reduce
     * 对处理结果进行Reducer的方法
     */
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context
                context) throws IOException, InterruptedException {
            int transCount = 0;
            String regDate = null;
            for (Text text : values) {
                String value = new String(text.copyBytes());
                String[] valueSplit = value.split(":");
                if (BusinessEnum.USER.getCode() == Integer.parseInt(valueSplit[1])) {
                    regDate = valueSplit[0];
                } else if (BusinessEnum.TRADE.getCode() == Integer.parseInt(valueSplit[1])) {
                    transCount = transCount + 1;
                }


            }
            context.write(key, new Text(String.format("%s\t%d%n", regDate, transCount)));
        }
    }

    /**
     * Map
     * 处理用户文本的Mapper
     */
    public static class SummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() > 0) {
                String[] str = line.split("\t");
                String regDate = str[1];
                int transCount = Integer.parseInt(str[2]);
                context.write(new Text(str[0].trim()),
                        new Text(String.format("%s\t%d%n", regDate, transCount)));
            }
        }
    }

    /**
     * Reduce
     * 二次job汇总的Reducer的方法
     */
    public static class SummaryReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context
                context) throws IOException, InterruptedException {
            String regDate = null;
            int transCount = 0;
            for (Text text : values) {
                String value = new String(text.copyBytes());
                String[] valueSplit = value.split("\t");
                if (StringUtils.isNotEmpty(valueSplit[0]) && !"null".equals(valueSplit[0])) {
                    regDate = valueSplit[0];
                }
                if (StringUtils.isNotEmpty(valueSplit[1]) && !"null".equals(valueSplit[1])) {
                    transCount = transCount + Integer.parseInt(valueSplit[1].
                            replaceAll("\n", "").
                            replaceAll("\r", ""));
                }
            }
            context.write(key, new Text(String.format("%s\t%d%n", regDate, transCount)));
        }
    }

    /**
     * Partition分区
     * 根据分区大小numPartition随机分配到相应的分区
     */
    public static class JoinPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // Random random = new Random();
            // int randomPartitioner = random.nextInt(numPartitions);
            // return randomPartitioner;
            return (((key).hashCode() + value.hashCode()) & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
