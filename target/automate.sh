#echo "done 6G"
#*/

for i in 100 200 300 400 500 600 700 800 900 1000
do
  for j in 2 3 4 5 6 
	do
  	spark-submit --class 'mapreducePredictionTwitter'$i --num-executors 4 --executor-memory $j'G' --conf "spark.locality.wait.node=0" --master yarn mapreduce-experiment-1.0-SNAPSHOT.jar
	sleep 120
	echo 'done execute data '$i' with executor memory of '$j'G'
done
done
