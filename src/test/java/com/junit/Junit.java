package com.junit;

import javax.sound.midi.Soundbank;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Junit {
    public static void main(String[] args) {
        Map<Integer,Integer> map=new HashMap<>();
        Random random = new Random();
        for(int i=0;i<10000;i++){
            int randomPartitioner = random.nextInt(10);
            if(map.containsKey(randomPartitioner))
                map.put(randomPartitioner,map.get(randomPartitioner)+1);
            else
                map.put(randomPartitioner,1);
        }
        System.out.println(map);

    }
}
