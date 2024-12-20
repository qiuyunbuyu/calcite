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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface to handle formatting between Kafka message and Calcite row.
 *
 * @param <K> type for Kafka message key,
 *           refer to {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG};
 * @param <V> type for Kafka message value,
 *           refer to {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG};
 *
 */
public interface KafkaRowConverter<K, V> {

  /**
   * Generates the row type for a given Kafka topic.
   *
   * 输入： 一个topic的名字
   * 输出：这个topic就被视为了一张table，table就有fields列，这个方法返回所有field的类型
   *
   * @param topicName Kafka topic name
   * @return row type
   */
  RelDataType rowDataType(String topicName);

  /**
   * Parses and reformats a Kafka message from the consumer,
   * to align with row type defined as {@link #rowDataType(String)}.
   *
   * 输入：一个消息message，这个message被视为了db中的一个“行”
   * 输出：这一“行”的file”列“
   *
   * @param message Raw Kafka message record
   * @return fields in the row
   */
  Object[] toRow(ConsumerRecord<K, V> message);
}
