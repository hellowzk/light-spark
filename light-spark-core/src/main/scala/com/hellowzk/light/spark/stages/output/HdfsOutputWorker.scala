package com.hellowzk.light.spark.stages.output

import com.hellowzk.light.spark.beans.output.HDFSOutputConfig
import com.hellowzk.light.spark.beans.{BaseConfig, HDFSOutputFormats}
import com.hellowzk.light.spark.constants.SysConstants
import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.uitils.HDFSUtils
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2019/11/25
 * <p>
 * 时间： 14:39
 * <p>
 * 星期： 星期一
 * <p>
 * 描述：HdfsOutput
 * <p>
 * 作者： zhaokui
 *
 **/
object HdfsOutputWorker {
  def apply: HdfsOutputWorker = new HdfsOutputWorker()
}

class HdfsOutputWorker extends BaseWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[HDFSOutputConfig]
    val constants = SysConstants.apply
    var df = ss.table(item.srcName)
    Option(item.partations).filter(part => part > 0).foreach(part => df = df.repartition(part))
    item.format match {
      case f if f == HDFSOutputFormats.txt.toString => HDFSUtils.apply.saveAsTXT(df, item.path, item.fs)(ss.sparkContext)
      case f if f == HDFSOutputFormats.lzo.toString => HDFSUtils.apply.saveAsLZO(df, item.path, item.fs)(ss.sparkContext)
      case f if f == HDFSOutputFormats.csv.toString => HDFSUtils.apply.saveAsCSV(df, item.path)(ss.sparkContext)
      case f if f == HDFSOutputFormats.json.toString => HDFSUtils.apply.saveAsJSON(df, item.path)(ss.sparkContext)
      case _ => throw new Exception(s"in outputs, unsupport format '${item.format}' for output '${item.srcName}'.")
    }
  }
}
