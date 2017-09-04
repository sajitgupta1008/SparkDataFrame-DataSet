import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class MatchStat(homeTeam: String, awayTeam: String, fthg: Long, ftag: Long, ftr: String)

object DataSetDemo extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Demo")
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val footBallCsv: Dataset[MatchStat] = spark.read.option("header", "true").option("inferSchema", "true").
    csv("/home/knoldus/home/knoldus/IdeaProjects/SparkDataFrameDemo/src/main/resources/D1.csv").select($"HomeTeam", $"AwayTeam",
    $"FTHG", $"FTAG", $"FTR").as[MatchStat]

  //no. of matches played by each team
  footBallCsv.select($"HomeTeam").union(footBallCsv.select($"AwayTeam")).groupBy($"HomeTeam" as "team").
    agg(count($"HomeTeam") as "#matches").show()

  //top 10 teams with win count
  footBallCsv.groupBy($"HomeTeam").agg(count(when($"FTR" === "H", 1)) as "count1").
    join(broadcast(footBallCsv).groupBy($"AwayTeam").agg(count(when($"FTR" === "A", 1)) as "count2"),
      $"HomeTeam" === $"AwayTeam", "outer").na.fill(0).withColumn("#wins", $"count1" + $"count2").
    select(when($"HomeTeam".isNotNull, $"HomeTeam").otherwise($"AwayTeam") as "team", $"#wins").
    sort($"#wins".desc).limit(10).show(false)

}