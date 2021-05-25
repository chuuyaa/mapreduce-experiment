# mapreduce-experiment
mapreduce-experiment for link prediction

In the RGD application, graph generation is different from both GC and OCD 
as it uses MapReduce own data abstraction. The proposed RGD for link prediction 
pseudocode is illustrated in Figure 6. Initially, the Spark context is created 
since we choose to deploy the algorithm in the Spark framework. The data is 
then loaded from HDFS. The first RDD is then created to represent graph 
records by lines. The second RDD is created with JavaPairRDD API to form 
a connecting link or graph based on input provided from step 3, the first 
RDD is also in edges form. To form the data in the form of a graph, 
we performed a groupByKey() operation on the combination of both RDD and 
triads which are formed. In comparing RGD to GC and OCD, they used a built-in 
GraphX library to form a graph that simplifies the graph formation operation.

Algorithm: Redundant Graph Detection Application
Input: Edgelists file from HDFS pathFromHDFS, parallelism n, Spark context sc
Output: overlapped communities
Begin
val lines = sc.textFile(pathFromHDFS
Edges = lines.flatMapToPair()
triads = edges.groupByKey()
trianglesWithDuplicates = triads.flatMap()
uniqueTriangles = trianglesWithDuplicates.distinct()
sout(uniqueTriangles)
Figure 6: RGD Pseudocode
