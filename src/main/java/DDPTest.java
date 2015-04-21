/**
 * Created by Andres on 21/04/2015.
 */

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Arrays;
import org.apache.spark.*;
import com.google.common.base.Optional;
import scala.Tuple2;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.util.regex.Pattern;

public class DDPTest {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        Boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            Logger.getRootLogger().setLevel(Level.OFF);
        }

        // Update the cumulative count function
        final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
            new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                @Override
                public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                    Integer newSum = state.or(0);
                    for (Integer value : values) {
                        newSum += value;
                    }
                    return Optional.of(newSum);
                }
            };


        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        ssc.checkpoint(".");

        @SuppressWarnings("unchecked")
        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 1),
                new Tuple2<String, Integer>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = ssc.sc().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> tweetStream = ssc.receiverStream(new NPTrackerReceiver("localhost", 3000));

        JavaDStream<String> words = tweetStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        // This will give a Dstream made of state (which is the cumulative count of the words)
        JavaPairDStream<String, Integer> stateDstream = wordsDstream.updateStateByKey(updateFunction,
                new HashPartitioner(ssc.sc().defaultParallelism()), initialRDD);

        stateDstream.print(100);

        ssc.start();
        ssc.awaitTermination();
    }
}
