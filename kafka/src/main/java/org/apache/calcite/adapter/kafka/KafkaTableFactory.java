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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;

/**
 * Implementation of {@link TableFactory} for Apache Kafka. Currently an Apache Kafka
 * topic is mapping to a STREAM table.
 */
public class KafkaTableFactory implements TableFactory<KafkaStreamTable> {
  public KafkaTableFactory() {
  }

  /**
   * 创建表, 返回KafkaStreamTable，KafkaStreamTable只有一个属性 KafkaTableOptions
   * 简单来说这个方法就是初始化好KafkaTableOptions，塞给KafkaStreamTable
   *
   * @param schema Schema this table belongs to
   * @param name Name of this table
   * @param operand The "operand" JSON property
   * @param rowType Row type. Specified if the "columns" JSON property.
   * @return KafkaStreamTable
   */
  @Override public KafkaStreamTable create(SchemaPlus schema,
      String name,
      Map<String, Object> operand,
      @Nullable RelDataType rowType) {

    // 构建KafkaTableOptions：kafka相关属性 + RowConverter
    final KafkaTableOptions tableOptionBuilder = new KafkaTableOptions();

    // 从operand的Map中取”bootstrap.servers“
    tableOptionBuilder.setBootstrapServers(
        (String) operand.getOrDefault(KafkaTableConstants.SCHEMA_BOOTSTRAP_SERVERS, null));

    // 从operand的Map中取”topic.name”
    tableOptionBuilder.setTopicName(
        (String) operand.getOrDefault(KafkaTableConstants.SCHEMA_TOPIC_NAME, null));

    final KafkaRowConverter rowConverter;

    // 取"row.converter"：自定义的
    if (operand.containsKey(KafkaTableConstants.SCHEMA_ROW_CONVERTER)) {
      String rowConverterClass = (String) operand.get(KafkaTableConstants.SCHEMA_ROW_CONVERTER);
      try {
        final Class<?> klass = Class.forName(rowConverterClass);
        rowConverter = (KafkaRowConverter) klass.getDeclaredConstructor().newInstance();
      } catch (InstantiationException | InvocationTargetException
          | IllegalAccessException | ClassNotFoundException
          | NoSuchMethodException e) {
        final String details =
            String.format(Locale.ROOT,
                "Failed to create table '%s' with configuration:\n"
                    + "'%s'\n"
                    + "KafkaRowConverter '%s' is invalid",
                name, operand, rowConverterClass);
        throw new RuntimeException(details, e);
      }
    } else {
    // 默认的：KafkaRowConverterImpl
      rowConverter = new KafkaRowConverterImpl();
    }
    tableOptionBuilder.setRowConverter(rowConverter);

    // 从operand的Map中取”consumer.params“: consumer属性
    if (operand.containsKey(KafkaTableConstants.SCHEMA_CONSUMER_PARAMS)) {
      tableOptionBuilder.setConsumerParams(
          (Map<String, String>) operand.get(KafkaTableConstants.SCHEMA_CONSUMER_PARAMS));
    }

    // 从operand的Map中取”consumer.cust“: consumer的类
    // calcite中测试的使用的是 [ org.apache.calcite.adapter.kafka.KafkaMockConsumer ]
    if (operand.containsKey(KafkaTableConstants.SCHEMA_CUST_CONSUMER)) {
      String custConsumerClass = (String) operand.get(KafkaTableConstants.SCHEMA_CUST_CONSUMER);
      try {
        tableOptionBuilder.setConsumer(
            (Consumer) Class.forName(custConsumerClass)
                .getConstructor(OffsetResetStrategy.class)
                .newInstance(OffsetResetStrategy.NONE));
      } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
          | InstantiationException | InvocationTargetException e) {
        final String details =
            String.format(Locale.ROOT,
                "Fail to create table '%s' with configuration:\n"
                    + "'%s'\n"
                    + "KafkaCustConsumer '%s' is invalid",
                name, operand, custConsumerClass);
        throw new RuntimeException(details, e);
      }
    }

    return new KafkaStreamTable(tableOptionBuilder);
  }
}
