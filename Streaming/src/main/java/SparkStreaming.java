import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SparkStreaming {
	private static final Logger logger = LogManager.getLogger(SparkStreaming.class);

	public static void main(String[] args) throws Exception {
		String[] jars = { System.getenv("JARS") };
		SparkConf conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
				.setSparkHome(System.getenv("SPARK_Home")).setJars(jars);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaDStream<String> stream = jssc.receiverStream(new MyReceiver("books.csv"));
		stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> rdd) throws TimeoutException, StreamingQueryException {
				JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
					@Override
					public Row call(String msg) {
						Row row = RowFactory.create(msg);
						return row;
					}
				});
		        StructType schema = new StructType()
		        		.add("name", DataTypes.StringType)
		        		.add("author", DataTypes.StringType)
		        		.add("user_rating", DataTypes.FloatType)
		        		.add("reviews", DataTypes.IntegerType)
		        		.add("Price", DataTypes.IntegerType)
		        		.add("year", DataTypes.IntegerType)
		        		.add("genre", DataTypes.StringType);
				SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
		        Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
		        msgDataFrame.count();
		              }
		        });

		        jssc.start();            
		        jssc.awaitTermination(); 
	}

}

class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}
}