package com.hellowzk.light.spark.stages

import com.alibaba.fastjson.JSON
import com.hellowzk.light.spark.beans.{BaseConfig, PersistTypes}
import com.hellowzk.light.spark.config.{BusConfig, CacheConstants}
import com.hellowzk.light.spark.uitils.{HDFSUtils, Logging}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * <p>
 * 日期： 2020/4/22
 * <p>
 * 时间： 13:58
 * <p>
 * 星期： 星期三
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
trait BaseWorker extends Logging {
  protected var variables: java.util.HashMap[String, String] = _

  def process(bean: BaseConfig)(implicit ss: SparkSession): Unit

  protected def initVariables()(implicit ss: SparkSession): Unit = {
    val str = ss.sparkContext.getConf.get("com.hellowzk.light.spark.variables")
    variables = JSON.parseObject(str, classOf[java.util.HashMap[String, String]])
  }

  /**
   * spark table 转 RDD
   *
   * @param tableName
   * @param ss
   * @return
   */
  def getRDDByTable(tableName: String)(implicit ss: SparkSession): RDD[Row] = {
    ss.table(tableName).rdd
  }

  /**
   * partation、缓存、持久化、打印
   *
   * @param process ProcessItemBean
   * @param ss      SparkSession
   */
  protected def afterProcess(process: BaseConfig)(implicit ss: SparkSession): Unit = {
    val resultTables = process.name.split(",", -1).map(_.trim)
    val conf = BusConfig.apply.getConfig()
    resultTables.filter(t => ss.catalog.tableExists(t))
      .foreach(tableName => {
        var df = ss.table(tableName)
        // repartition
        if (process.partations > 0) {
          df = df.repartition(process.partations)
          df.createOrReplaceTempView(tableName)
        }
        // cache
        if (process.cache) {
          doCache(tableName)
        }
        // stored, 如果没有配置 cache, 先 cache, 再 store
        if (process.store && conf.isDebug) {
          if (!process.cache) {
            doCache(tableName)
          }
          val persistType = conf.getPersistType
          persistType match {
            case persistType if persistType == PersistTypes.hdfs.toString =>
              val path = new Path(BusConfig.apply.getConfig().persistDir, tableName).toString
              HDFSUtils.apply.saveAsCSV(df, path)(ss.sparkContext)
              logger.info(s"persist '$tableName' to hdfs '$path' success for debug.")
            case persistType if persistType == PersistTypes.hive.toString =>
              ss.table(tableName).write.mode(SaveMode.Overwrite).format("Hive").saveAsTable(s"${conf.getPersistHiveDb}.$tableName")
              logger.info(s"persist table '$tableName' to hive '${conf.getPersistHiveDb}.$tableName' success for debug.")
            case _ =>
          }
        }
        // show
        if (process.show > 0 && conf.getEnableShow) {
          println(s"show table '$tableName':")
          ss.table(tableName).show(process.show, false)
        }
      })
  }

  private def doCache(tableName: String)(implicit ss: SparkSession): Unit = {
    ss.sql(s"cache table $tableName")
    CacheConstants.tables.append(tableName)
    logger.info(s"cached table '$tableName' success.")
  }
}
