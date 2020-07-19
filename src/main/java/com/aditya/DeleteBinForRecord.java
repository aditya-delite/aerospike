package com.aditya;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.Optional;

public class DeleteBinForRecord {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "set1", "latest");
        Bin bin = Bin.asNull("bin3");
        client.put(null, key,bin);
        System.out.println("deleted bin with name bin 3");
        Record record = client.get(null, key, "bin3");
        System.out.println("trying to fetch record with bin 3 ...");
        Optional.ofNullable(record).orElseThrow(()->new NullPointerException("Record not found"));
    }
}
