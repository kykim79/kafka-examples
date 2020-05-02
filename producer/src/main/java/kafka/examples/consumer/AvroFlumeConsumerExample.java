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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
            String serializer = res.getString("serializer");

            String groupId = res.getString("group.id");
            String inputDelimiter = res.getString("id");
            String outputDelimiter = res.getString("od");

            final SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");


            Properties consumerConfig = new Properties();
            consumerConfig.put("group.id", groupId);
            consumerConfig.put("bootstrap.servers",brokerList);
            consumerConfig.put("auto.offset.reset", "latest");
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig);
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(1000);

                try {
                    for (ConsumerRecord<Object, Object> record : records) {
                        AvroFlumeEvent event = deserialize((byte[])record.value());

                        Map<CharSequence, CharSequence> headers = event.getHeaders();

                        StringBuilder sb = new StringBuilder();
                        headers.forEach((key, value) -> {
                            sb.append(key).append(": ");
                            if (key.equals(new Utf8("timestamp"))) {
                                sb.append(df.format(new Date(Long.parseLong(value.toString())))).append(", ");
                            } else {
                                sb.append(value).append(", ");
                            }
                        });

                        if(inputDelimiter == null) {
                            sb.append("body: ").append(StandardCharsets.UTF_8.decode(event.getBody()));
                        } else {
                            String[] cols = String.valueOf(StandardCharsets.UTF_8.decode(event.getBody())).split(inputDelimiter);
                            sb.append("body: ").append(StringUtils.join(cols, outputDelimiter == null ? " | " : outputDelimiter));
                        }

                        System.out.println(sb.toString());
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

        parser.addArgument("--id").action(store())
                .required(false)
                .type(String.class)
                .metavar("STRING")
                .help("input delimiter");

        parser.addArgument("--od").action(store())
                .required(false)
                .type(String.class)
                .metavar("STRING")
                .help("output delimiter");

        return parser;
    }
}