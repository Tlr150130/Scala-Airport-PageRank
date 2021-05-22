// Databricks notebook source
// libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import spark.implicits._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// load data - flights in January 2021
val filepath = "/FileStore/tables/411618378_T_T100D_SEGMENT_US_CARRIER_ONLY.csv"
val data = spark
  .read.option("header", "true")
  .option("inferSchema", "true")
  .csv(filepath)
  .select("ORIGIN","DEST")
display(data)

// COMMAND ----------

// FLow
// 1. Set all the pageranks/outlines
// 2. reduce by key (x, y) -> x+y to get page rank
// 3. update main data (all origins get PageRank updates) with new page rank
// 4. Repeat 20 times

// COMMAND ----------

// class to hold all the datasets for reassignment during for loop
class dfHolder(){
  var data = spark.emptyDataFrame
  var df = spark.emptyDataFrame
  var df1 = spark.emptyDataFrame
  var df2 = spark.emptyDataFrame
  var df3 = spark.emptyDataFrame
  var df4 = spark.emptyDataFrame
}

// COMMAND ----------

// Initial PageRank is 10.0 for the 1st iteration
var holder = new dfHolder()
val alpha = 0.15
val n = data.select("dest").distinct().count() // number of vertices (dest has more vertices)
val outlines = data.groupBy($"ORIGIN").count() // number of outlines for each vertex
val outlinesFloat = outlines.withColumn("count",col("count").cast("Double"))
val dataWithOutlines = data.join(outlinesFloat, Seq("ORIGIN"))
holder.data = data
holder.df = dataWithOutlines.withColumn("PageRank", lit(10.0))
display(holder.df)

// COMMAND ----------

// Iterate 20 times
for (i <- 1 to 20) {
  holder.df1 = holder.df.select($"ORIGIN", $"DEST", ($"PageRank"/$"count").as("PagerankDiv"))
  holder.df2 = holder.df1.select("DEST", "PagerankDiv").groupBy("DEST").sum() // adds all the parent's PageRanks/Outlines
  holder.df3 = holder.df2.select($"DEST".as("Node"),((lit(alpha/n) + ($"sum(PagerankDiv)"*(1-alpha))).as("PageRank"))) // calculates PageRank
  holder.df4 = holder.data.join(holder.df3, holder.data.col("ORIGIN") === holder.df3.col("Node")) // updates new PageRank
  holder.df = holder.df4.join(outlinesFloat, Seq("ORIGIN")) // adds the parent's outlines
}

// COMMAND ----------

// display the results for top 10 page rank
display(holder.df.select("Node","PageRank").dropDuplicates("Node").sort($"PageRank".desc))

// COMMAND ----------


