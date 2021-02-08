import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class ProducerService {
    public static void sendMessage(String topicName, Message msg) {
        //create the topic if not existing
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient client = AdminClient.create(adminProps);
        client.createTopics(Arrays.asList(new NewTopic(topicName, 1, (short)1)));

        //create the producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Message.MessageSerializer.class);
        KafkaProducer<String, Message> producer = new KafkaProducer<>(properties);

        //send the message
        producer.send(new ProducerRecord<>(topicName, msg));
        producer.close();

    }
}
