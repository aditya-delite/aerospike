package com.aditya.avroDemo;

import com.aditya.employee.Employee;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

public class EmployeeRepositoryAvro {
    public static void main(String[] args) throws IOException {
        Employee employee = new Employee(200, "Rajesh", "Developer");
        ObjectMapper mapper = new ObjectMapper();
        String empString = mapper.writeValueAsString(employee);
        System.out.println("Employee-> " + empString);
        //serializeEmployee(employee);
        fetchEmployeeFromDb();
    }

    private static void persistToDatabase() throws IOException {
        WritePolicy policy = new WritePolicy();
        policy.expiration = -1;
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(com.aditya.avro.Employee.getClassSchema());
        DataFileReader fileReader = new DataFileReader<>(new File("src/main/resources/test.txt"), reader);
        while (fileReader.hasNext()) {
            GenericData.Record genericRecord = (GenericData.Record) fileReader.next();
            System.out.println("generic record: " + genericRecord);
            Integer empId = (Integer) genericRecord.get("empId");

            Key key = new Key("test", "employee", String.valueOf(empId));
            Bin bin = new Bin("data", genericRecord);
            client.put(null, key, bin);
        }

    }

    private static void serializeEmployee(Employee employee) throws IOException {
        com.aditya.avro.Employee employee1 = com.aditya.avro.Employee.newBuilder()
                .setEmpId(employee.getEmpId())
                .setEmpName(employee.getEmpName())
                .setDesignation(employee.getDesignation())
                .build();
        DatumWriter<com.aditya.avro.Employee> writer = new SpecificDatumWriter<>(
                com.aditya.avro.Employee.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    com.aditya.avro.Employee.getClassSchema(), stream);
            writer.write(employee1, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {

        }
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "employee", String.valueOf(employee.getEmpId()));
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
    }

    public static void fetchEmployeeFromDb() throws IOException {
        AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
        Key key = new Key("test", "employee", "200");
        Record record = client.get(null, key);
        byte[] data = (byte[]) record.bins.get("data");
        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(com.aditya.avro.Employee.getClassSchema(), new String(data));
        DatumReader<com.aditya.avro.Employee> reader = new SpecificDatumReader<>(com.aditya.avro.Employee.getClassSchema());
        com.aditya.avro.Employee read = reader.read(null, jsonDecoder);
        System.out.println(read);

        //ByteArrayInputStream bis = new ByteArrayInputStream(data);
//        ObjectInputStream in = null;
        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
            Object o = in.readObject();
            System.out.println(o);
        }
        catch(Exception e){

        }

        }
    }
