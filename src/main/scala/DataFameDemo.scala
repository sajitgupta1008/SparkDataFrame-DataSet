import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFameDemo extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Demo")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val footBallCsv: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").
    csv("/home/knoldus/home/knoldus/IdeaProjects/SparkDataFrameDemo/src/main/resources/D1.csv")

  //no. of matched played by each team as hometeam
  footBallCsv.groupBy("HomeTeam").count().union(footBallCsv.select("AwayTeam")
    .except(footBallCsv.select("HomeTeam")).withColumn("count", lit(0))).show(false)

  //top 10 teams with win percentage
  val jointDF = footBallCsv.groupBy(col("HomeTeam")).agg(count(when(col("FTR") === "H", 1)) as "count1", count(col("FTR")) as "total1").
    join(broadcast(footBallCsv).groupBy(col("AwayTeam")).agg(count(when(col("FTR") === "A", 1)) as "count2", count(col("FTR")) as "total2"),
      $"HomeTeam" === $"AwayTeam", "outer").na.fill(0)

  jointDF.withColumn("win%", ($"count1" + $"count2") / ($"total1" + $"total2") * 100).
    select(when($"HomeTeam".isNotNull, $"HomeTeam").otherwise($"AwayTeam") as "team", $"win%").
    sort($"win%".desc).limit(10).show(false)

}
