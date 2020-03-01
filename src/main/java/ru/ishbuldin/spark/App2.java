package ru.ishbuldin.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.stream.Stream;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .getOrCreate();
        String[] paths = {"D:/data/emp/hr/", "D:/data/emp/developers/", "D:/data/emp/managers/"};
        JavaPairRDD<String, String> javaPairRDD = sc.wholeTextFiles(Stream.of(paths).reduce((s1, s2) -> s1 + "," + s2).get());

        StructType structType = new StructType();
        structType = structType.add("department", DataTypes.StringType, true);
        structType = structType.add("json", DataTypes.StringType, true);
        StructType json = new StructType();
        json = json.add("name", DataTypes.StringType);
        json = json.add("salary", DataTypes.StringType);

        JavaRDD<Row> javaRDD = javaPairRDD.map(a -> RowFactory.create(getDepartment(a._1()), a._2()));
        Dataset<Row> df = spark.createDataFrame(javaRDD, structType);
        df = df.withColumn("parsed_json", functions.from_json(functions.col("json"), json))
                .select("parsed_json.*", "department");
        df.show();
        df.select(functions.max(functions.col("salary").cast(DataTypes.IntegerType))).show();
        df.select(functions.sum(functions.col("salary").cast(DataTypes.IntegerType))).show();
    }

    private static String getDepartment(String file) {
        return file.split("/")[4];
    }
}