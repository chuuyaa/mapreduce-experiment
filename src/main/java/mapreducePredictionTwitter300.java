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

//https://www.jetbrains.com/help/idea/creating-and-running-your-first-java-application.html

public class mapreducePredictionTwitter300 {

    public static void main(String[] args) throws Exception {

        // Step 3: create context object
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkMapReduce").setMaster("yarn"));
	sc.hadoopConfiguration().set("mapred.max.split.size", "37500");

        // Step 4: create the first RDD from input path HDFS input text file representing a graph
        // records are representing as JavaRDD<String>
        JavaRDD<String> lines = sc.textFile("/user/hadoop/data/graphx/300k.txt");
        //JavaRDD<String> lines = sc.textFile("/Users/User/IdeaProject/mapreduceexperiment/src/main/resources/40k.txt");

        // Step 5: Create a new JavaPairRDD for all edges
        // which includes (source, destination) and (destination, source)
        JavaPairRDD<Long, Long> edges = lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(String s) {
                String[] nodes = s.split("\t");
                long start = Long.parseLong(nodes[0]);
                long end = Long.parseLong(nodes[1]);
                // Note that edges must be reciprocal
                return Arrays.asList(new Tuple2<Long, Long>(start, end),
                        new Tuple2<Long, Long>(end, start)).iterator();
            }
        });

        // Step 6: create a new JavaPairRDD which will generate triads
        JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey();

        // For debugging, we collect all triads object and display them:
        List<Tuple2<Long, Iterable<Long>>> debug1 = triads.collect();
        for (Tuple2<Long, Iterable<Long>> t2 : debug1) {
            System.out.println("debug1 t2._1=" + t2._1);
            System.out.println("debug1 t2._2=" + t2._2);
        }

        // Step 7: create a new JavaPairRDD which will generate possible triads
        JavaPairRDD<Tuple2<Long, Long>, Long> possibleTriads = triads.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Tuple2<Long, Long>, Long>() {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, Long>> call(Tuple2<Long, Iterable<Long>> s) {
                // s._1 = Long (as a key)
                // s._2 = Iterable<Long> (as values)
                Iterable<Long> values = s._2;
                // we assume that no node has an ID of zero
                List<Tuple2<Tuple2<Long, Long>, Long>> result = new ArrayList<Tuple2<Tuple2<Long, Long>, Long>>();

                // Generate possible triads.
                for (Long value : values) {
                    Tuple2<Long, Long> k2 = new Tuple2<Long, Long>(s._1, value);
                    Tuple2<Tuple2<Long, Long>, Long> k2v2 = new Tuple2<Tuple2<Long, Long>, Long>(k2, 0l);
                    result.add(k2v2);
                }

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
                    }
                }

                return result.iterator();
            }
        });

        // To debug step 7, collect all possibleTriads objects and display them
        List<Tuple2<Tuple2<Long, Long>, Long>> debug2 = possibleTriads.collect();
        for (Tuple2<Tuple2<Long, Long>, Long> t2 : debug2) {
            System.out.println("debug2 t2._1=" + t2._1);
            System.out.println("debug2 t2._2=" + t2._2);
        }

        // Step 8: create a new JavaPairRDD, which will generate triangles
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> triadsGrouped = possibleTriads.groupByKey();

        // To debug step 8
        //List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> debug3 = triadsGrouped.collect();
        //for (Tuple2<Tuple2<Long, Long>, Iterable<Long>> t2 : debug3) {
        //    System.out.println("debug3 t2._1=" + t2._1);
        //    System.out.println("debug3 t2._2=" + t2._2);
        //}

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
        });

        // debug step 9
        //System.out.println("=== Triangles with Duplicates ===");
        //List<Tuple3<Long, Long, Long>> debug4 = trianglesWithDuplicates.collect();
        //for (Tuple3<Long, Long, Long> t3 : debug4) {
        //    //System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
        //    System.out.println("t3=" + t3);
        //}

        // Step 10: eliminate duplicate triangles and create unique triangles
        JavaRDD<Tuple3<Long, Long, Long>> uniqueTriangles = trianglesWithDuplicates.distinct();

        // debug step 10
        //System.out.println("=== Unique Triangles ===");
        //List<Tuple3<Long, Long, Long>> output = uniqueTriangles.collect();
        //for (Tuple3<Long, Long, Long> t3 : output) {
        //    //System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
        //    System.out.println("t3=" + t3);
        //}

        // done
        sc.close();

        //
        System.exit(0);
    }
//    private static JavaSparkContext sc;
//
//    public mapreducePredictionTwitter(JavaSparkContext sc){
//        this.sc = sc;
//    }
//
//    public void generateGraph(JavaSparkContext sc) {
//
//    }

//    public static void main(String[] args) throws Exception {
//        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkMapReduce").setMaster("local"));
//        mapreducePredictionTwitter job = new mapreducePredictionTwitter(sc);
//    }
}

