package ru.ishbuldin.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

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
        List<String> files = new ArrayList<>(Arrays.asList(paths));
        JavaPairRDD<String, String> javaPairRDD = sc.wholeTextFiles("D:/data/emp/hr/,D:/data/emp/developers/,D:/data/emp/managers/");

        javaPairRDD.foreach(a -> System.out.println(a._1 + " " + a._2));

        StructType structType = new StructType();
        structType = structType.add("department", DataTypes.StringType, true);
        structType = structType.add("json", DataTypes.StringType, true);

        StructType json = new StructType();
        json = json.add("name", DataTypes.StringType);
        json = json.add("salary", DataTypes.StringType);

        JavaRDD<Row> javaRDD = javaPairRDD.map(a -> RowFactory.create(getDepartment(a._1()), a._2()));
        Dataset<Row> df = spark.createDataFrame(javaRDD, structType);
        df.show();

        df.select(functions.from_json(functions.col("json"), json)).show();
    }

    private static String getDepartment(String file) {
        return file.split("/")[4];
    }
}