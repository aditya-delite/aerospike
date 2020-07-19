package com.aditya;

import com.aerospike.client.*;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;

public class OperationDemo {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "demo", "operatekey");
        Bin bin1 = new Bin("strBin", "hello");
        Bin bin2 = new Bin("intBin", 5);
        WritePolicy policy = new WritePolicy();
        policy.expiration = -1;
        client.put(policy, key, bin1, bin2);
        System.out.println("before operation");
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.includeBinData = true;
        Record record = client.get(queryPolicy, key);
        record.bins.entrySet().forEach(entry -> System.out.println(entry.getKey() + "- " + entry.getValue()));
        //now using operation
        Bin b1 = new Bin(bin1.name, " world");
        Bin b2 = new Bin(bin2.name, 5);
        Record re = client.operate(policy, key, Operation.append(b1), Operation.add(b2), Operation.get());
        System.out.println("after operation");
        re.bins.entrySet().forEach(entry -> System.out.println(entry.getKey() + "- " + entry.getValue()));
    }
}
