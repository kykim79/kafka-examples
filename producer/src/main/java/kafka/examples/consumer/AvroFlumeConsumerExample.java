/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.examples.consumer;

import com.google.common.collect.Lists;
import kafka.examples.producer.Util;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.SerializationException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class AvroFlumeConsumerExample {



    public static void main(String[] args) {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String brokerList = res.getString("bootstrap.servers");
            String topic = res.getString("topic");
            int epoch_column = 1;
            try {
                epoch_column = res.getInt("epoch_column");
            } catch (Exception e) {
                System.out.println("epoch_column option is not used. default value is 1");
            }

            String serializer = res.getString("serializer");

            String groupId = res.getString("group.id");

            final SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            final String DEFAULT_SEPARATOR = "\036";

            Properties consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
//            consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig);
            consumer.subscribe(Collections.singletonList(topic));
//            Map<String, List<PartitionInfo>> topicInfos = consumer.listTopics();
//            List<PartitionInfo> partitions = topicInfos.get(topic);
//            for(PartitionInfo partition : partitions) {
//                TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
//                List<TopicPartition> topicPartitions = Lists.newArrayList(topicPartition);
//                consumer.assign(topicPartitions);
//                consumer.seekToEnd(topicPartitions);
//            }

            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(1000);

                try {
                    for (ConsumerRecord<Object, Object> record : records) {
                        AvroFlumeEvent event = deserialize((byte[])record.value());

                        Map<CharSequence, CharSequence> headers = event.getHeaders();

                        StringBuilder sb = new StringBuilder();
                        headers.entrySet().forEach(entry -> {
                            sb.append(entry.getKey()).append(": ");
                            if(entry.getKey().equals(new Utf8("timestamp"))) {
                                sb.append(df.format(new Date(Long.parseLong(entry.getValue().toString())))).append(", ");
                            } else {
                                sb.append(entry.getValue()).append(", ");
                            }
                        });
//                        sb.append(StandardCharsets.UTF_8.decode(event.getBody()));

                        String body = String.format("%s", StandardCharsets.UTF_8.decode(event.getBody()));
                        sb.append(body);

                        String[] epoch =  Util.getNthColumn(body, epoch_column).split("\\.");
                        try {
                            String logTime = df.format(new Date(Long.parseLong(epoch[0] + epoch[1].substring(0, 3))));
                            sb.append(String.format(" log-time:%s", logTime));
                            System.out.println(String.format("%s",sb.toString()));
                        } catch (Exception e) {
                            e.printStackTrace();
                            for (String s : epoch) {
                                System.out.println("epoch index exception: " + s);
                            }
                        }
                    }
                } catch (SerializationException e) {
                    System.out.printf("exception: %s\n", e.getMessage());
                } finally {
                    consumer.commitSync();
                }
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

    }

    private static AvroFlumeEvent deserialize(byte[] bytes)  {
        SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader();
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        AvroFlumeEvent e = new AvroFlumeEvent();
        reader.setSchema(e.getSchema());
        try {
            e = reader.read(e, decoder);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        return e;
    }

    /**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("simple-producer")
                .defaultHelp(true)
                .description("This example is to demonstrate kafka producer capabilities");

        parser.addArgument("--group.id").action(store())
                .required(true)
                .type(String.class)
                .metavar("STRING")
                .help("specifies the name of the consumer group");

        parser.addArgument("--bootstrap.servers").action(store())
                .required(true)
                .type(String.class)
                .metavar("BROKER-LIST")
                .help("comma separated broker list");

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--serializer").action(store())
                .required(false)
                .setDefault("byte")
                .type(String.class)
                .choices(Arrays.asList("byte", "kryo"))
                .metavar("BYTE/KRYO")
                .help("use byte array or kryo serializer");

        parser.addArgument("--epoch_column").action(store())
              .required(false)
              .setDefault(1)
              .type(Integer.class)
              .metavar("INTEGER")
              .help("epoch column index to convert to timestamp");

        return parser;
    }
}