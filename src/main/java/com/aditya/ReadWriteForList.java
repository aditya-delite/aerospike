package com.aditya;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.Arrays;
import java.util.List;

public class ReadWriteForList {

    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "demo", "listKey");
        List<String> list = Arrays.asList("amit", "aditya", "ankur");
        Bin bin = new Bin("names", list);
        //client.put(null, key, bin);
//        System.out.println("successfully written to aerospike");
        Record record = client.get(null, key);
        List<String> names = (List<String>) record.getList("names");
        names.stream().forEach(System.out::println);
    }
}
