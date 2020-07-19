package com.aditya;

import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;

import java.util.ArrayList;
import java.util.List;

public class OperationOnList {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        List<Integer> l1 = new ArrayList<>();
        l1.add(1);
        l1.add(2);
        l1.add(3);
        l1.add(4);
        List<Integer> l2 = new ArrayList<>();
        l2.add(100);
        l2.add(200);
        l2.add(300);
        l2.add(400);
        List<List<Integer>> l3 = new ArrayList<>();
        l3.add(l1);
        l3.add(l2);
        List<Value> l4 = new ArrayList<>();
        l4.add(Value.get(5000));
        Key key = new Key("test", "demo","operateOnList");
        client.put(null, key, new Bin("names",l3));
//        Record record = client.operate(null, key, ListOperation.appendItems("names",l4),Operation.get("names"));
//        Record record = client.operate(null, key, ListOperation.append("names",Value.get(-100)),Operation.get("names"));
        Record record = client.operate(null, key, ListOperation.insert("names",1,Value.get(500)),Operation.get("names"));

        System.out.println(record.bins);
    }
}
