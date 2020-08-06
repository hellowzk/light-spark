package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.stages.custom.CustomBaseInput
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2020/7/6
 * <p>
 * 时间： 17:43
 * <p>
 * 星期： 星期一
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 **/
class Process1 extends CustomBaseInput {
  override def process(rdd: RDD[String], name: String)(implicit ss: SparkSession): Unit = {
    val filter = rdd.map(_.split(",", -1).map(_.trim)).filter(_.size == 3)
      .map(cols => (cols(0), cols(1), cols(2)))
    import ss.implicits._
    filter.toDF("name", "gend", "age").createOrReplaceTempView(name)
  }
}
