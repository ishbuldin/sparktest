package ru.ishbuldin.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class App 
{
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
        Map<String, Dataset<Row>> dfs = new HashMap<>();
        for (String file : files) {
            Dataset<Row> df = spark.read().option("multiline", true).json(file)
                    .withColumn("department", functions.lit(file.split("/")[3]));
            dfs.put(file, df);
        }

        StructType structType = new StructType();
        structType = structType.add("name", DataTypes.StringType, true);
        structType = structType.add("salary", DataTypes.StringType, true);
        structType = structType.add("department", DataTypes.StringType, false);

        Dataset<Row> ds = spark.createDataFrame(sc.emptyRDD(), structType);
        for (Dataset<Row> row : dfs.values()) {
            ds = ds.union(row);
        }
        ds.show();
        ds.coalesce(1).write().option("header", true).mode(SaveMode.Overwrite).csv("D:/data/result");
        ds.select(functions.max("salary")).show();
        ds.select(functions.sum(functions.col("salary"))).show();
    }
}