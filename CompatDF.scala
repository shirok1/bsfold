package tech.shiroki

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import sun.misc.Signal

import java.util.concurrent.Semaphore

case class CPEL(cell: Long, phone: Long, folding: List[(Long, Long)])

case class CellLogRecord(cell: Long, timestamp: Long, enterOrLeave: Short, phone: Long)

object CompatDF extends App with Logging {

  val spark = SparkSession
    .builder()
    .appName("Spark Infection")
    //    .master("local[14]")
    .getOrCreate()

  import spark.implicits._

  //  val cdinfoFile = "hdfs://master/user/root/cdinfo.txt"
  //  val cdinfoFile = "/home/shiroki/Documents/cdinfo.txt"
  val cdinfoFile = args(0)
  //  val infectedFile = "hdfs://master/user/root/infected08.txt"
  //  val infectedFile = "/home/shiroki/Downloads/infected08.txt"
  val infectedFile = args(1)

  val resultFile = args(1)

  val ds = spark.read.schema(StructType(Seq(
    StructField("cell", LongType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("enterOrLeave", ShortType, nullable = false),
    StructField("phone", LongType, nullable = false))
  )).csv(cdinfoFile).as[CellLogRecord]

  val spans = ds.groupByKey(x => (x.cell, x.phone)).mapGroups(
    (group, infos) => {
      CPEL(group._1, group._2, infos.toList.sortBy(_.timestamp).foldLeft((Nil: List[(Long, Long)], None: Option[Long], 0)) {
        case ((mergedList, None, 0), e) if e.enterOrLeave == 1 =>
          (mergedList, Some(e.timestamp), 1)
        case ((mergedList, Some(lastEnter), 1), e) if e.enterOrLeave == 2 =>
          ((lastEnter, e.timestamp) :: mergedList, None, 0)
        case ((mergedList, maybeEnter, stack), e) if e.enterOrLeave == 1 =>
          (mergedList, maybeEnter, stack + 1)
        case ((mergedList, maybeEnter, stack), e) if e.enterOrLeave == 2 =>
          (mergedList, maybeEnter, stack - 1)
      }._1)
    }
  ).cache()

  val sortedSpan = spans.select(
    $"cell".as("spanCell"),
    $"phone",
    explode($"folding").as("folding")
  ).sort($"spanCell").cache()
  logger.error(s"对 cdinfo.txt 预处理完成, 共 ${sortedSpan.count()} 条记录")

  for (i <- 5 to 0 by -1) {
    Thread.sleep(1000)
    logger.error(s"$i 秒后开始读取 infected.txt")
  }

  val infected = spark.read.schema(StructType(Seq(
    StructField("infectedPhone", LongType, nullable = false)
  ))).csv(infectedFile)

  val infectedSpans = spans.join(infected, $"phone" === $"infectedPhone", "cross").as[CPEL]

  val result = infectedSpans.select(
    $"cell",
    explode($"folding").as("infectedFolding")
  ).join(
    sortedSpan,
    $"cell" === $"spanCell", "cross"
  ).filter(
    "(folding._2 >= infectedFolding._1) AND (folding._1 <= infectedFolding._2)"
  ).select($"phone").distinct().sort($"phone").cache()

  logger.error(s"共有 ${result.count()} 人需要标记为红码")
  result.show()

  result.write.text(resultFile)

  val semaphore = new Semaphore(0)

  Signal.handle(new Signal("INT"), _ => {
    logger.info("收到 Ctrl+C 信号, 退出")
    semaphore.release()
  })

  semaphore.acquire()

}
