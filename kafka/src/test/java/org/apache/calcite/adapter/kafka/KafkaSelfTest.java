/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.kafka;

import java.sql.*;
import java.util.Properties;

public class KafkaSelfTest {
  public static void main(String[] args) throws ClassNotFoundException, SQLException {
//      KafkaRowConverter<byte[], byte[]> rowConverter = new KafkaRowConverterImpl();
//      String bootstrapServers = "kafka9001.eniot.io:9092";
//      String topicNAme = "topic-fishyu";
//      Map<String, String> consumerParams = new HashMap<>();
//      consumerParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//          StringDeserializer.class.getName());
//      consumerParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//        StringDeserializer.class.getName());
//      consumerParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//      consumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, "test-calcite-kafka");
//      consumerParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//      KafkaTableOptions kafkaTableOptions = new KafkaTableOptions();
//      kafkaTableOptions.setBootstrapServers(bootstrapServers);
//      kafkaTableOptions.setConsumerParams(consumerParams);
//      kafkaTableOptions.setRowConverter(rowConverter);
//      kafkaTableOptions.setTopicName(topicNAme);
//
//      KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(kafkaTableOptions);

    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"defaultSchema\": \"KAFKA\",\n" +
            "  \"schemas\": [\n" +
            "    {\n" +
            "      \"name\": \"KAFKA\",\n" +
            "      \"tables\": [\n" +
            "        {\n" +
            "          \"name\": \"ZK_ERRORLOG\",\n" +
            "          \"type\": \"custom\",\n" +
            "          \"factory\": \"org.apache.calcite.adapter.kafka.KafkaTableFactory\",\n" +
            "          \"operand\": {\n" +
            "\"bootstrap.servers\":\"kafka9001.eniot.io:9092\",\n" +
            "\"topic.name\":\"zk_errorlog\",\n" +
            "            \"consumer.params\": {\n" +
"              \"key.deserializer\": \"org.apache.kafka.common.serialization" +
            ".StringDeserializer\",\n" +
            "              \"value.deserializer\": \"org.apache.kafka.common.serialization" +
            ".StringDeserializer\",\n" +
            "              \"group.id\":\"testcalcite\""+
            "            } \n"+
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select STREAM * from KAFKA.zk_errorlog");

    while(resultSet.next()){
      System.out.println(resultSet.getString("MSG_KEY_BYTES"));
      System.out.println(resultSet.getString("MSG_VALUE_BYTES"));
    }

  }
}
