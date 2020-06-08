import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.*;

public class SimpleKafkaApi {

    public static void main(String[] args) throws Exception {
        String topicName, message;

        //create a topic
        System.out.println("Enter topic name");
        topicName = new Scanner(System.in).nextLine();

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        AdminClient adminClient = AdminClient.create(config);
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        ArrayList<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);
        adminClient.createTopics(newTopics);

        //create Consumer
        Properties propsConsumer = new Properties();
        propsConsumer.put("bootstrap.servers", "localhost:9092");
        propsConsumer.put("group.id", "test");
        propsConsumer.put("enable.auto.commit", "false");
        propsConsumer.put("auto.commit.interval.ms", "1000");
        propsConsumer.put("session.timeout.ms", "30000");
        propsConsumer.put("auto.offset.reset","earliest");
        propsConsumer.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        propsConsumer.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(propsConsumer);
        consumer.subscribe(Arrays.asList(topicName));
        System.out.println("Subscribed to topic " + topicName);

        //create Producer
        Properties propsProducer = new Properties();
        propsProducer.put("bootstrap.servers", "localhost:9092");
        propsProducer.put("acks", "all");
        propsProducer.put("retries", 0);
        propsProducer.put("batch.size", 16384);
        propsProducer.put("linger.ms", 1);
        propsProducer.put("buffer.memory", 33554432);
        propsProducer.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer
                <String, String>(propsProducer);

         Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    //consumer reads message from topic
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records)
                        System.out.printf("Your message (offset = %d) = %s\n", record.offset(), record.value());
                }
            }
        });
        thread.start();

        while(true) {
            System.out.println("Enter a message");
            message = new Scanner(System.in).nextLine();
            //thread.sleep(1000);
            if (message.equals("exit")==true) break;
            //producer sends message to topic
            producer.send(new ProducerRecord<String, String>(topicName,
                        message));
            thread.sleep(1000);
           // System.out.println("Message sent successfully");
        }
        producer.close();

    }
}
//}