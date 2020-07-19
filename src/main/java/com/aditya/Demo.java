package com.aditya;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class Demo {
    public static void main(String[] args) {
        replaceValue();
//        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
//        Key key = new Key("test", "set1", "hii");
//        Bin b1 = new Bin("bin3", "java");
////        Bin b2 = new Bin("bin2", "Aditya");
//        client.put(null, key, b1);
//        System.out.println("written to aerospike");
//        Record record = client.get(null, key);
//        record.bins
//                .entrySet()
//                .stream()
//                .forEach(entry -> System.out.println("key" + entry.getKey() + " value" + entry.getValue()));
    }

    public static void replaceValue(){
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "set1", "latest");
        Bin b = new Bin("bin3","java 14");
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction= RecordExistsAction.REPLACE;
        client.put(policy,key,b);
        //
        Record record = client.get(null, key);
        Object hii = record.bins.get("latest");
        System.out.println(hii);

    }
}
