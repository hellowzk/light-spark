package com.hellowzk.light.spark.stages.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * <p>
 * 日期： 2019/11/29
 * <p>
 * 时间： 15:42
 * <p>
 * 星期： 星期五
 * <p>
 * 描述： count 函数
 * <p>
 * 作者： zhaokui
 *
 **/
class MySumUDF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("type", StringType, true)))
  }

  // 聚合操作时，中间更新，所处理的数据的类型
  override def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("type", LongType, true)))
  }

  // 最终函数返回值的类型
  override def dataType: DataType = {
    DataTypes.LongType
  }

  //多次运行 相同的输入总是相同的输出，确保一致性
  override def deterministic: Boolean = {
    true
  }

  /**
   * 为每个分组的数据执行初始化值
   * 两个部分的初始化：
   *   1.在map端每个RDD分区内，在RDD每个分区内 按照group by 的字段分组，每个分组都有个初始化的值：a=0
   *   2.在reduce 端给每个group by 的分组做初始值:a=0
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
  }

  /** 每个组，有新的值进来的时候，进行分组对应的聚合值的计算
   * input就是输入的数据，传进来name，放到Row中
   * buffer就是上一步的初始值
   * 拿到0号位，来一个就+1
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1 //初始值+1
  }

  /**
   * 最后merger的时候，在各个节点上的聚合值，要进行merge，也就是合并
   * buffer1拿某一组的初始值：a=0   buffer2就是a1组的值：2   --->  0+2=2
   * 下一波，buffer1就是2，buffer2就是a2组的值：1   --->2+1=3
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
  }

  // 最后返回一个最终的聚合值要和dataType的类型一一对应
  //buffer：每个组的总和
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Long](0)
  }
}
