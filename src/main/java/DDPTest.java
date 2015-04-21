/**
 * Created by Andres on 21/04/2015.
 */

import com.google.common.collect.Lists;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.regex.Pattern;

public class DDPTest {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        Boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> tweetStream = ssc.receiverStream(new NPTrackerReceiver("localhost", 3000));

        JavaDStream<String> words = tweetStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        words.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
