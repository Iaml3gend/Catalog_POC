import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.util.concurrent.TimeoutException;


public class KafkaMessagesConsume {
    public static Dataset<Row> readKafkaData(SparkSession spark,String kafkaServer,String topicName){
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", topicName).option("includeHeaders","true")
                .load();
    }

    public static StreamingQuery printMessagesToConsole(Dataset<Row> topicData) throws TimeoutException {
      return topicData
                .writeStream()
                .outputMode("append")
                .format("console").option("truncate","False")
                .start();

    }

    public static StreamingQuery writeMessageToMongo(Dataset<Row> topicData) throws TimeoutException {

        return topicData
                .writeStream()
                .outputMode(OutputMode.Update())
                .foreach(new ForeachWriter<Row>() {
                    private MongoClient mongoClient;
                    private MongoCollection<Document> collection;

                    @Override
                    public boolean open(long partitionId, long version) {
                        mongoClient = MongoClients.create(Constants.MONGODB_URI);
                        MongoDatabase db = mongoClient.getDatabase(Constants.DATABASE_NAME);
                        collection = db.getCollection(Constants.COLLECTION_NAME);
                        return true;
                    }

                    @Override
                    public void process(Row row) {
                        Document doc = new Document();
                        doc.append("key", row.getString(3))
                                .append("value", row.getString(4));
                        collection.insertOne(doc);
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        mongoClient.close();
                    }
                })
                .start();
    }


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {


        SparkSession spark = SparkSession.builder()
                .appName("KafkaExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> empTopicData = readKafkaData(spark,Constants.KAFKA_SERVER, Constants.TOPIC_NAME);

        /* Reading Topic name, partition, offset value and data from Kafa Messages  */
        empTopicData = empTopicData.selectExpr("CAST(topic as STRING)", "CAST(partition as STRING)",
                "CAST(offset as STRING)","CAST(value as STRING)");

        /* Defining a java object with Custom Schema according to the message */
        StructType emp_schema = new StructType().add("name", DataTypes.StringType).add("country",
                DataTypes.StringType);

         /* Parsing the above message into different columns such as name and country column
          * Capturing the entire json object and initializing it with emp_schema and renaming it to emp_data
          */
        empTopicData = empTopicData.select(functions.col("topic"),functions.col("partition"),
                functions.col("offset"), functions.from_json(functions.col("value"),
                        emp_schema).alias("emp_data"));

        /* Now all the json attributes are in different columns
         * Using emp_data.* we are printing all the json attributes
         */
        empTopicData  = empTopicData.select("topic", "partition","offset","emp_data.*");

        /* Filtering the countries, so we only get employees from India */
        empTopicData = empTopicData.filter(functions.col("country").equalTo("India"));

        /* Calling printMessagesToConsole Method and passing appropriate topic name */
        StreamingQuery printEmpDataToConsole = printMessagesToConsole(empTopicData);

        /* Calling writeMessageToMongo Method and passing appropriate topic name */
        StreamingQuery writeEmpDataToMongo = writeMessageToMongo(empTopicData);


        printEmpDataToConsole.awaitTermination();

        writeEmpDataToMongo.awaitTermination();
    }
}
