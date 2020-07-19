package com.aditya;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

import java.util.Random;

public class WriteDemo {
    public static void main(String[] args) {
        int lowKeyVal = 0;
        int numKeys = 15;
        if (args.length > 2 || args.length == 1) {
            System.out.println("invalid number of arguements..");
            System.exit(1);
        } else if (args.length == 2) {
            try {
                lowKeyVal = Integer.parseInt(args[0]);
                numKeys = Integer.parseInt(args[1]);
            } catch (Exception e) {
                System.out.println("parameter should be integer type");
                System.exit(1);
            }
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            for (int i = lowKeyVal; i < numKeys; i++) {
                Key key = new Key("test", "demo", i);
                Random rand = new Random();
                int nextInt = rand.nextInt();
                Bin b1 = new Bin("intBin", nextInt);
                Bin b2 = new Bin("strBin", "Str" + nextInt);
                client.put(new WritePolicy(), key, b1, b2);
            }
        }
    }
}
