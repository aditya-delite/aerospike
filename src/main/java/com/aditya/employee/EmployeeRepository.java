package com.aditya.employee;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmployeeRepository {
    public static void main(String[] args) throws JsonProcessingException {
        WritePolicy policy = new WritePolicy();
        policy.expiration = -1;
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Employee employee = new Employee(100, "Abhishek", "Software Engineer");
        ObjectMapper mapper = new ObjectMapper();
        String value = mapper.writeValueAsString(employee);
        System.out.println("emp## " + value);
        Key key = new Key("test", "employee", "" + employee.getEmpId());
        Bin bin = new Bin("data", value);
        client.put(policy, key, bin);
        System.out.println("written to aerospike successfully");
        Record record = client.get(null, key);
        String data = (String) record.bins.get("data");
        Employee dbEmployee = mapper.readValue(data, Employee.class);
        System.out.println(dbEmployee);
    }
}
