package com.rickiyang.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 上午10:20 2018/11/21
 * @Modified by:
 * @Description:
 **/
public class Serialization {

    public static void main(String[] args) {

        encode();
        decode();
        nonCoding();
        nonCodingDecode();
    }

    /**
     * 序列化
     */
    public static void encode() {
        try {
            User user1 = new User();
            user1.setName("Tom");
            user1.setAge(7);
            user1.setSex(1);

            DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
            DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            dataFileWriter.create(user1.getSchema(), new File("user.avro"));
            dataFileWriter.append(user1);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 反序列化
     */
    public static void decode() {
        try {
            DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
            DataFileReader<User> dataFileReader = new DataFileReader<>(new File("user.avro"), userDatumReader);
            User user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 不使用对象的序列化
     */
    public static void nonCoding() {
        try {
            ClassLoader classLoader = Serialization.class.getClassLoader();
            String avscFilePath = classLoader.getClass().getResource("/user.avsc").getPath();
            Schema schema = new Schema.Parser().parse(new File(avscFilePath));

            GenericRecord user1 = new GenericData.Record(schema);
            user1.put("name", "Tony");
            user1.put("age", 18);

            GenericRecord user2 = new GenericData.Record(schema);
            user2.put("name", "Ben");
            user2.put("age", 3);
            user2.put("sex", 1);

            File file = new File("user.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 不使用对象的反序列化
     */
    public static void nonCodingDecode() {
        try {
            ClassLoader classLoader = Serialization.class.getClassLoader();
            String avscFilePath = classLoader.getClass().getResource("/user.avsc").getPath();
            Schema schema = new Schema.Parser().parse(new File(avscFilePath));
            File file = new File("user.avro");
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
