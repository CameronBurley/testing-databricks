// Databricks notebook source
val df = spark.read.option("header",true).csv("/FileStore/tables/ads-4.csv").select("Year", "Product/Title")
display(df)

// COMMAND ----------

def cleanQuotes(str: String) = str.substring(1, str.tail.indexWhere(_.equals('"')))
val cleanUdf= udf(cleanQuotes _)

// COMMAND ----------

val dfNoMovies = df.select("Year","Product/Title").filter($"Product/Title".contains("\"\""))
val dfNoQuotes = dfNoMovies.withColumn("Product/Title", cleanUdf($"Product/Title"))
display(dfNoQuotes)

// COMMAND ----------

display(dfNoQuotes)

// COMMAND ----------

val commercials = dfNoQuotes.select("Product/Title").collect.map(x => x(0).toString)

// COMMAND ----------

val listOfKeeping = List("Audi", "Hyundai", "Kia", "Toyota", "Walmart", "T-Mobile", "GMC", "Jeep", "Porsche", "Facebook")
def myfil(str: String): Boolean = {
  listOfKeeping.indexWhere(x => str.contains(x)) match {
    case -1 => return false
    case _ => return true
  }
}

// COMMAND ----------

val keepingdf = dfNoQuotes.select("Year","Product/Title").filter(x => myfil(x(1).toString))
keepingdf.show

// COMMAND ----------

def toBrand(str: String): String = {
  listOfKeeping.indexWhere(x => str.contains(x)) match {
    case x => return listOfKeeping(x)
  }
}
val toBrandUdf = udf(toBrand _)


// COMMAND ----------

import org.apache.spark.sql.functions._
val dfNormal = keepingdf.withColumn("Product/Title", toBrandUdf($"Product/Title")).distinct.sort(asc("Year"))
dfNormal.show

// COMMAND ----------

dfNormal.write.csv("/FileStore/tables/brands_clean.csv")
