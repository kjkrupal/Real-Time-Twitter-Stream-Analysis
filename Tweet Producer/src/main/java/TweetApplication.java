import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TweetApplication {

    ResourceBundle rb;
    Properties properties;


    public TweetApplication() {
        rb = ResourceBundle.getBundle("Application");
        String server = "127.0.0.1:9092";
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {

        TweetApplication tweetApplication = new TweetApplication();

        String headers = tweetApplication.getHeaders();
        List <String> tweets = tweetApplication.getTweets();

        tweetApplication.streamTweets(headers, tweets);

    }

    public void produceTweet(String tweet) {
        KafkaProducer <String, String> producer = producer = new KafkaProducer<String, String>(properties);
        ProducerRecord <String, String> record = new ProducerRecord<String, String>("tweets", tweet);
        producer.send(record);
        producer.flush();
        producer.close();

    }

    public void streamTweets(String headers, List <String> tweets) {

        while(true) {
            tweets.forEach(line -> {

                String[] header = headers.split(",");
                String[] tweet = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                Map<String, String> map = new HashMap<>();

                try {
                    for(int i = 0; i < header.length; i++) {
                        map.put(header[i], tweet[i]);
                    }

                    JSONObject jsonObject = new JSONObject(map);
                    String json = jsonObject.toString();
                    produceTweet(json);
                    Thread.sleep(2000);
                }
                catch (Exception e) {

                }

            });
        }

    }

    public List<String> getTweets() throws IOException{

        String tweetFileName = rb.getString("tweet-file");
        Stream <String> fileStream = Files.lines(Paths.get(tweetFileName));

        List<String> tweets = fileStream.filter(line -> {
            String id = line.split(",")[0];
            return StringUtils.isNumeric(id);
        }).collect(Collectors.toList());

        return tweets;
    }

    public String getHeaders() throws IOException {
        String tweetFileName = rb.getString("tweet-file");
        Stream <String> firstLine = Files.lines(Paths.get(tweetFileName));
        String headers = firstLine.findFirst().get();
        return headers;
    }


}
