package com.examination.zhangfang.wordcount;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 1、词频统计
 *      现在有大量文本文件（100+），预计一共涉及单词数量1W左右。需要由5个线程并发计算，全部计算后做结果合并，选出出现频率最高的100个单词及对应次数。
 */
public class WordFrequencyCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordFrequencyCount.class);

    //定义文件路径队列
    public static List<String> filePathList = new ArrayList<>(256);
    //定义线程固定大小线程池
    public static ExecutorService wordFrequencyCountThreadPool = Executors.newFixedThreadPool(5);



    @Test
    public static void main(String[] args) throws Exception {
        Vector<Map<String, Integer>> wordFrequencyCountVector = new Vector<>();
        //这里可以初始化filePathList
        String rootPath = WordFrequencyCount.class.getResource("/").getFile();
        filePathList.add(rootPath + "wordcount/WordFrequencyCount001.txt");
        filePathList.add(rootPath + "wordcount/WordFrequencyCount002.txt");
        filePathList.add(rootPath + "wordcount/WordFrequencyCount003.txt");
        filePathList.add(rootPath + "wordcount/WordFrequencyCount004.txt");
        filePathList.add(rootPath + "wordcount/WordFrequencyCount005.txt");

        //定义同步工具类
        CountDownLatch countDownLatch = new CountDownLatch(filePathList.size());
        for (String filePath : filePathList) {
            wordFrequencyCountThreadPool.submit(() -> {
                try {
                    //对单个文档进行分词统计
                    Map<String, Integer> singleFileCountMap = new ConcurrentHashMap<String, Integer>();
                    File file = new File(filePath);
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                    try {
                        String line = null;
                        while (null != (line = bufferedReader.readLine())) {
                            String[] words = line.split(",");
                            for (String word : words) {
                                singleFileCountMap.put(word, singleFileCountMap.containsKey(word) ? singleFileCountMap.get(word) + 1 : 1);
                            }
                        }
                    } finally {
                        bufferedReader.close();
                    }
                    wordFrequencyCountVector.add(singleFileCountMap);
                } catch (Exception e) {
                    LOGGER.error("{}",e.getMessage(),e);
                }
                //每个任务执行完成后调用countDown
                countDownLatch.countDown();
            });
        }
        //等待所有任务执行完成后再继续执行后续代码
        countDownLatch.await();
        //对所有文档分词统计进行合并
        Map<String, Integer> wordFrequencyCountMap = new HashMap<>();
        wordFrequencyCountVector.forEach(singleFileCountMap -> {
            for (Map.Entry<String, Integer> entry : singleFileCountMap.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                wordFrequencyCountMap.put(key, wordFrequencyCountMap.containsKey(key) ? wordFrequencyCountMap.get(key) + value : value);
            }
        });

        // 对wordFrequencyCountMap进行排序
        List<Map.Entry<String, Integer>> list = new ArrayList<>(wordFrequencyCountMap.entrySet());
        Collections.sort(list, (o1, o2) -> {
            // 降序排序
            return o2.getValue().compareTo(o1.getValue());
        });
        // 输出前100的单词和数量
        for (int i=0;i<list.size() && i<100;i++) {
            Map.Entry<String, Integer> mapping=list.get(i);
            LOGGER.info(mapping.getKey() + ":" + mapping.getValue());
        }
    }
}
