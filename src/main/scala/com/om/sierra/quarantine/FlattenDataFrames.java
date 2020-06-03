package com.om.sierra.quarantine;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class FlattenDataFrames {

    public static final String ADDRESS_COMPANY = "company_address_line";
    public static final String CITY_HOME = "home_city";
    public static final String CITY_COMPANY = "company_city";
    public static final String ADDRESS_HOME = "home_address_line";

    public static void main (String args[]) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSample")
                .master("local[*]")
                .getOrCreate();


        //Read file, replace as per your machine
        Dataset<Row> ds = spark.read().format("csv").option("header", false).load("C:\\Users\\Sharad\\Desktop\\Test.csv").toDF("emp_id", "emp_name");
        System.out.println("Hi Running Java from here");
        Dataset<Row> ds1 = spark.read().format("csv").option("header", false).load("C:\\Users\\Sharad\\Desktop\\Test1.csv").toDF("emp_id", "address_type", "address_line", "city");
        ds.show();
        ds1.show();

        Dataset<Row> merged = ds.join(ds1 , "emp_id");

        merged.show();

        // Define UDF's
        spark.sqlContext().udf().register("AddressTypeHome", (UDF2<String, String, String>)
                (columnValue1, columnValue2) -> {
                    return columnValue1.equalsIgnoreCase("home") ? columnValue2 : null;
                }, DataTypes.StringType);
        spark.sqlContext().udf().register("AddressTypeCompany", (UDF2<String, String, String>)

                (columnValue1, columnValue2) -> {
                    return columnValue1.equalsIgnoreCase("company") ? columnValue2 : null;
                }, DataTypes.StringType);

        // Add New Columns
        merged = merged.withColumn(ADDRESS_HOME,
                functions.callUDF("AddressTypeHome", merged.col("address_type"), merged.col("address_line")));
        merged = merged.withColumn(ADDRESS_COMPANY,
                functions.callUDF("AddressTypeCompany", merged.col("address_type"), merged.col("address_line")));
        merged = merged.withColumn(CITY_HOME,
                functions.callUDF("AddressTypeHome", merged.col("address_type"), merged.col("city")));
        merged = merged.withColumn(CITY_COMPANY,
                functions.callUDF("AddressTypeCompany", merged.col("address_type"), merged.col("city")));

        // Drop Old ones
        merged = merged.drop("address_type");
        merged = merged.drop("city");
        merged = merged.drop("address_line");

        // Complement Data
        merged.groupBy("emp_id","emp_name").
                agg(functions.first(ADDRESS_HOME,true).alias(ADDRESS_HOME),functions.first(ADDRESS_COMPANY,true).alias(ADDRESS_COMPANY),
                        functions.first(CITY_HOME,true).alias(CITY_HOME), functions.first(CITY_COMPANY,true).alias(CITY_COMPANY)).
                orderBy("emp_id").
                show();
    }

    public static String getHomeValue (String val){
        if (val.equalsIgnoreCase("Home")){
            return val;
        }
        return null;
    }

    public static Column getCompanyValue (String val){
        if (val.equalsIgnoreCase("Company")){
            return new Column("val");
        }
        return null;
    }


}
