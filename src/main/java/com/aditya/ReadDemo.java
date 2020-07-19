package com.aditya;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class ReadDemo {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "set1", "hello");
        Record record = client.get(null, key,"bin2");
        record.bins.entrySet().stream().forEach(entry -> System.out.println(entry.getKey() + "-" + entry.getValue()));
    }
}
