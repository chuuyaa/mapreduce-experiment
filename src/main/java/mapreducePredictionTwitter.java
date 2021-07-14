// step1: import required classes and interfaces
import org.apache.spark.SparkConf;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple3;
import java.util.*;

public class mapreducePredictionTwitter {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("dude, i need at least one parameter");
        }
        String path = args[0];
        //String path = "C:/tmp/100k.txt";
        // Step 3: create context object
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkMapReduce").setMaster("local[*]"));
        sc.hadoopConfiguration().set("mapred.max.split.size", "25000");

        // Step 4: create the first RDD from input path HDFS input text file representing a graph
        // records are representing as JavaRDD<String>
        JavaRDD<String> lines = sc.textFile(path);

        // Step 5: Create a new JavaPairRDD for all edges
        // which includes (source, destination) and (destination, source)
        JavaPairRDD<Long, Long> edges = lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(String s) {
                String[] nodes = s.split("\\s");
                long start = Long.parseLong(nodes[0]);
                long end = Long.parseLong(nodes[1]);
                // Note that edges must be reciprocal
                return Arrays.asList(new Tuple2<Long, Long>(start, end),
                        new Tuple2<Long, Long>(end, start)).iterator();
            }
        });

        System.gc();

        // Step 6: create a new JavaPairRDD which will generate triads
        JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey().cache();

        // Step 7: create a new JavaPairRDD which will generate possible triads
        JavaPairRDD<Tuple2<Long, Long>, Long> possibleTriads = triads.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Tuple2<Long, Long>, Long>() {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, Long>> call(Tuple2<Long, Iterable<Long>> s) {
                // s._1 = Long (as a key)
                // s._2 = Iterable<Long> (as values)
                Iterable<Long> values = s._2;
                // we assume that no node has an ID of zero
                List<Tuple2<Tuple2<Long, Long>, Long>> result = new ArrayList<Tuple2<Tuple2<Long, Long>, Long>>();

                // RDD's values are immutable, so we have to copy the values
                // copy values to valuesCopy
                List<Long> valuesCopy = new ArrayList<Long>();
                for (Long item : values) {
                    valuesCopy.add(item);
                }
                Collections.sort(valuesCopy);

                // Generate possible triads.
                for (int i = 0; i < valuesCopy.size() - 1; ++i) {
                    for (int j = i + 1; j < valuesCopy.size(); ++j) {
                        Tuple2<Long, Long> k2 = new Tuple2<Long, Long>(valuesCopy.get(i), valuesCopy.get(j));
                        Tuple2<Tuple2<Long, Long>, Long> k2v2 = new Tuple2<Tuple2<Long, Long>, Long>(k2, s._1);
                        result.add(k2v2);
                        k2 = null;
                        k2v2=null;
                    }
                }

                return result.iterator();
            }
        }).cache();

        // Step 8: create a new JavaPairRDD, which will generate triangles
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> triadsGrouped = possibleTriads.groupByKey().cache();

        // Step 9: create a new JavaPairRDD, which will generate all triangles
        JavaRDD<Tuple3<Long, Long, Long>> trianglesWithDuplicates = triadsGrouped.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, Long>, Iterable<Long>>, Tuple3<Long, Long, Long>>() {
            @Override
            public Iterator<Tuple3<Long, Long, Long>> call(Tuple2<Tuple2<Long, Long>, Iterable<Long>> s) {
                // s._1 = Tuple2<Long,Long> (as a key) = "<nodeA><,><nodeB>"
                // s._2 = Iterable<Long> (as a values) = {0, n1, n2, n3, ...} or {n1, n2, n3, ...}
                // note that 0 is a fake node, which does not exist
                Tuple2<Long, Long> key = s._1;
                Iterable<Long> values = s._2;
                // we assume that no node has an ID of zero

                List<Long> list = new ArrayList<Long>();
                boolean haveSeenSpecialNodeZero = false;
                for (Long node : values) {
                    if (node == 0) {
                        haveSeenSpecialNodeZero = true;
                    } else {
                        list.add(node);
                    }
                }

                List<Tuple3<Long, Long, Long>> result = new ArrayList<Tuple3<Long, Long, Long>>();
                if (haveSeenSpecialNodeZero) {
                    if (list.isEmpty()) {
                        // no triangles found
                        // return null;
                        return result.iterator();
                    }
                    // emit triangles
                    for (long node : list) {
                        long[] aTraingle = {key._1, key._2, node};
                        Arrays.sort(aTraingle);
                        Tuple3<Long, Long, Long> t3 = new Tuple3<Long, Long, Long>(aTraingle[0],
                                aTraingle[1],
                                aTraingle[2]);
                        result.add(t3);
                    }
                } else {
                    // no triangles found
                    // return null;
                    return result.iterator();
                }

                return result.iterator();


            }
        }).cache();

        // Step 10: eliminate duplicate triangles and create unique triangles
        JavaRDD<Tuple3<Long, Long, Long>> uniqueTriangles = trianglesWithDuplicates.distinct();
        // Print out unique triangles
        System.out.println("=== Unique Triangles ===");
        List<Tuple3<Long, Long, Long>> output = uniqueTriangles.collect();
        for (Tuple3<Long, Long, Long> t3 : output) {
            //System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
            System.out.println("t3=" + t3);
        }

        // done
        sc.close();

        //
        System.exit(0);
    }
}

