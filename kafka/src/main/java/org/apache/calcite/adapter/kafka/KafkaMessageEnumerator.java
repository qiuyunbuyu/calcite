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

import org.apache.calcite.linq4j.Enumerator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Enumerator to read data from {@link Consumer},
 * and converted into SQL rows with {@link KafkaRowConverter}.
 *
 * @param <K> Type for Kafka message key,
 *           refer to {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG};
 * @param <V> Type for Kafka message value,
 *           refer to {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG};
 */
public class KafkaMessageEnumerator<K, V> implements Enumerator<@Nullable Object[]> {
  // kafka consumer
  final Consumer consumer;
  // kafka message与calcite row中转换抽象
  final KafkaRowConverter<K, V> rowConverter;
  private final AtomicBoolean cancelFlag;

  // runtime
  // message列表
  private final Deque<ConsumerRecord<K, V>> bufferedRecords = new ArrayDeque<>();
  // 当前message
  private @Nullable ConsumerRecord<K, V> curRecord;

  KafkaMessageEnumerator(final Consumer consumer,
      final KafkaRowConverter<K, V> rowConverter,
      final AtomicBoolean cancelFlag) {
    this.consumer = consumer;
    this.rowConverter = rowConverter;
    this.cancelFlag = cancelFlag;
  }

  /**
   * It returns an Array of Object, with each element represents a field of row.
   * 返回curRecord抽象出的row，对应的所有fields
   */
  @Override public Object[] current() {
    return rowConverter.toRow(requireNonNull(curRecord, "curRecord"));
  }

  /**
   * advanced to the next element
   * 取下一个
   * @return 是否取成功
   */
  @Override public boolean moveNext() {
    if (cancelFlag.get()) {
      return false;
    }
    // 拉取一批消息
    while (bufferedRecords.isEmpty()) {
      pullRecords();
    }
    // 从bufferedRecords，更新curRecord
    curRecord = bufferedRecords.removeFirst();
    return true;
  }

  private void pullRecords() {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord record : records) {
      bufferedRecords.add(record);
    }
  }

  @Override public void reset() {
    this.bufferedRecords.clear();
    pullRecords();
  }

  @Override public void close() {
    consumer.close();
  }
}
