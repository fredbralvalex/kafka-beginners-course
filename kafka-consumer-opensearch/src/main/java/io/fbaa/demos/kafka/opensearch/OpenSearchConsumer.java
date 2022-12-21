package io.fbaa.demos.kafka.opensearch;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupIp = "consumer-opensource-demo";

        // creating consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIp);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // creating the consumer

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        // GSon library
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // first create an OpenSearch Client
        RestHighLevelClient openSourceClient = createOpenSearchClient();

        // create our kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // create the index on the OpenSource if it does not exist
        try (openSourceClient; consumer) {

            boolean indexExists = openSourceClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSourceClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exists!");
            }

            // subscribe the consumer
            consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();

                log.info("Received " + recordCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

                    //send the record into opensearch

                    //strategy 1

                    // define on ID usnig kafka record coordinates
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        // strategy 2
                        // we extract the id from the JSON value
                        String id = extractId(record.value());

                        // send the records into OpenSource
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON).id(id);

//                        IndexResponse response = openSourceClient.index(indexRequest, RequestOptions.DEFAULT);
//                        log.info("" + response.getId());

                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {

                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSourceClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records.");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been committed.");

                }


            }
        }


        // main code logic

        // close things
    }


}
