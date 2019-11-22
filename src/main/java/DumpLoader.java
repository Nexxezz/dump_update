import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class DumpLoader {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark dump update example")
                .config("spark.master", "local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> result = spark.createDataFrame(Collections.emptyList(), new StructType().add("user_id",LongType,true).add("repo_id",LongType,true).add("created_at",StringType,true));
        String path = args[0];
        File archive_directory = new File(path);
        if (archive_directory.exists()) {
            File[] jsonFiles = archive_directory.listFiles();
            for (File file : jsonFiles) {
                Dataset<Row> batch = spark.read().json(path+file.getName());
                Dataset<Row> watchers = batch.filter(col("type").equalTo("WatchEvent")).select(col("actor.id").as("user_id"), col("repo.id").as("repo_id"), col("created_at"));
                result = result.union(watchers);
            }
        } else System.out.println("Incorrect directory path");
        result.write().option("header", "true").csv( path+"result");
    }
}

