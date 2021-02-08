import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Stack;
import java.util.function.Consumer;

public class ConsumerService {
    private KafkaConsumer<String, Message> consumer;
    public Stack<String> pendingTopics;
    private String groupId;
    public ConsumerService(String groupId) {
        pendingTopics = new Stack<>();
        this.groupId = groupId;
    }
    public KafkaConsumer<String, Message> getConsumer() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Message.MessageDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }
    public void readMessage(List<String> topics, File file, Consumer<String> onTopicAddition) throws IOException {
        consumer = getConsumer();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
        consumer.subscribe(topics);
        boolean topicAdded = false;
        while(!topicAdded) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                try {
                    bufferedWriter.write(record.value().getMessage() + "\n");
                    bufferedWriter.flush();
                    consumer.commitAsync();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            while(!pendingTopics.empty()) {
                String topicName = pendingTopics.pop();
                topics.add(topicName + "-grp");
                onTopicAddition.accept(topicName);
                topicAdded = true;
            }
        }
        consumer.close();
        readMessage(topics, file, onTopicAddition);
    }
}
