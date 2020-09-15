/**
 * @author <a href="mailto:liuxiaolin6@cmbc.com.cn">Liu,Xiaolin</a>
 */
package com.hellowzk.light.spark.uitils

import java.io.{File, IOException}
import java.nio.file.Paths

import com.hadoop.compression.lzo.LzopCodec
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 15:08
 * <p>
 * 星期：
 * <p>
 * 描述：hdfs 操作工具类
 * <p>
 * 作者： zhaokui
 *
 **/
object HDFSUtils {
  def apply: HDFSUtils = new HDFSUtils()
}

class HDFSUtils extends Logging {
  implicit def string2path(value: String): Path = new Path(value)

  implicit def path2string(value: Path): String = value.toString

  /**
   * 列出目录下的所有item
   *
   * @param parentPath 要操作的路径
   * @param sc         SparkContext
   * @return items
   */
  def listDir(parentPath: String)(implicit sc: SparkContext): Array[String] = withFS(fs => fs.listStatus(parentPath).filter(_.isDirectory).map(d => d.getPath.toString))

  /**
   * 判断目录是否为 directory
   *
   * @param path path
   * @param sc   SparkContext
   * @return
   */
  def isDir(path: String)(implicit sc: SparkContext): Boolean = withFS(fs => fs.isDirectory(new Path(path)))


  /**
   * 创建目录
   *
   * @param path 要创建的目录
   * @param sc   SparkContext
   */
  def mkdir(path: String)(implicit sc: SparkContext): Unit = withFS(fs => fs.mkdirs(new Path(path)))

  /**
   * 创建空文件
   *
   * @param path      要创建的文件全路径
   * @param recursive 是否递归创建路径
   * @param sc        SparkContext
   */
  def touchz(path: String, recursive: Boolean = true)(implicit sc: SparkContext): Unit = withFS(fs => fs.create(new Path(path)).close())

  /**
   * 删除目录
   *
   * @param path      要删除的目录
   * @param recursive 是否递归删除目录
   * @param sc        SparkContext
   */
  def remove(path: String, recursive: Boolean = true)(implicit sc: SparkContext): Unit = withFS(fs => fs.delete(new Path(path), recursive))

  /**
   * 判断路径是否存在
   *
   * @param path 路径
   * @param sc   SparkContext
   * @return 是否存在
   */
  def exists(path: String)(implicit sc: SparkContext): Boolean = withFS(fs => fs.exists(new Path(path)))

  /**
   * 获取FileSystem
   *
   * @param action FileSystem 要做的操作
   * @param sc     SparkContext
   * @tparam T 操作后返回的类型
   * @return
   */
  private def withFS[T](action: FileSystem => T)(implicit sc: SparkContext): T = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    action(fs)
  }

  /**
   * 保存 DataFrame 为 txt 类型
   *
   * @param df       DataFrame
   * @param fullPath fullPath
   * @param fs       列分隔符
   * @param tag      successTag
   * @param sc       SparkContext
   */
  def saveAsTXT(df: DataFrame, fullPath: String, fs: String = "\u0001", tag: String = null)(implicit sc: SparkContext): Unit = {
    save(fullPath, path => {
      df.rdd.map(_.toSeq.mkString(fs))
        .saveAsTextFile(path)
    }, successTag = tag)
  }

  /**
   * 保存 DataFrame 为 txt lzo 类型
   *
   * @param df       DataFrame
   * @param fullPath fullPath
   * @param fs       列分隔符
   * @param tag      successTag
   * @param sc       SparkContext
   */
  def saveAsLZO(df: DataFrame, fullPath: String, fs: String = "\u0001", tag: String = null)(implicit sc: SparkContext): Unit = {
    save(fullPath, path => {
      df.rdd.map(_.toSeq.mkString(fs))
        .saveAsTextFile(path, classOf[LzopCodec])
    }, successTag = tag)
  }

  /**
   * 保存 DataFrame 为 csv 类型
   *
   * @param df         DataFrame
   * @param fullPath   fullPath
   * @param partitions partitions
   * @param tag        successTag
   * @param sc         SparkContext
   */
  def saveAsCSV(df: DataFrame, fullPath: String, partitions: Int = 0, tag: String = null)(implicit sc: SparkContext): Unit = {
    save(fullPath, path => {
      val part = if (partitions == 0)
        df
      else
        df.repartition(partitions)
      part.write.format("com.databricks.spark.csv").option(
        "header", "true"
      ).save(path)
    }, successTag = tag)
  }

  /**
   * 保存 DataFrame 为 json 类型
   *
   * @param df         DataFrame
   * @param fullPath   fullPath
   * @param partitions partitions
   * @param tag        successTag
   * @param sc         SparkContext
   */
  def saveAsJSON(df: DataFrame, fullPath: String, partitions: Int = 0, tag: String = null)(implicit sc: SparkContext): Unit = {
    save(fullPath, path => {
      val part = if (partitions == 0)
        df
      else
        df.repartition(partitions)
      part.toJSON.rdd.saveAsTextFile(path)
    }, successTag = tag)
  }

  /**
   * 保存 DataFrame 为 parquet 类型
   *
   * @param df         DataFrame
   * @param fullPath   fullPath
   * @param partitions partitions
   * @param tag        successTag
   * @param sc         SparkContext
   */
  def saveAsParquet(df: DataFrame, fullPath: String, partitions: Int = 0, tag: String = null)(implicit sc: SparkContext): Unit = {
    save(fullPath, path => {
      val part = if (partitions == 0)
        df
      else
        df.repartition(partitions)
      part.write.parquet(path)
    }, successTag = tag)
  }

  /**
   * 保存数据
   *
   * @param path       要保存到的目标路径
   * @param doSave     具体的保存方法
   * @param overwrite  目标存在是否覆盖
   * @param successTag 成功标志
   * @param sc         SparkContext
   */
  private def save(path: String, doSave: String => Unit, overwrite: Boolean = true, successTag: String)(implicit sc: SparkContext): Unit = {
    val dir: String = Paths.get(path).normalize.toString
    Option(dir).filter(d => exists(d)).foreach(d => {
      if (overwrite) {
        logger.warn(s"already exists, try to purge")
        remove(dir)
        logger.warn(s"purged")
      } else {
        throw new IOException(s"target path already exisits: $dir")
      }
    })
    doSave(path)
    Option(dir).filter(d => !exists(d)).foreach(d => {
      logger.warn("empty result set, try to create an empty result file")
      touchz(d)
    })
    logger.info(s"saved at: $dir")
    Option(successTag).foreach(tag => {
      val tag = Paths.get(dir, successTag).normalize.toString
      logger.info(s"success tag: $tag")
      mkdir(tag)
      logger.info(s"success tag created")
    })
  }

  /**
   * 加载classpath数据
   *
   * @param file     数据路径
   * @param fs       行分隔符
   * @param nullable 为true时，允许file不存在返回空RDD
   * @param sc       SparkContext
   * @return
   */
  def loadClasspathFile(file: String, fs: String, nullable: Boolean = true)(implicit sc: SparkContext): RDD[String] = {
    val fullPath = ResourceUtil.apply.get(file).getPath
    sc.parallelize(FileUtils.readLines(new File(fullPath)).toSeq)
  }

  def loadClasspathFile(file: String)(implicit sc: SparkContext): RDD[String] = {
    val fullPath = ResourceUtil.apply.get(file).getPath
    sc.parallelize(FileUtils.readLines(new File(fullPath)).toSeq)
  }

  def loadHdfsTXT(file: String, fs: String, nullable: Boolean = true)(implicit sc: SparkContext): RDD[String] = {
    var loadRDD: RDD[String] = null
    var strings = Array.empty[String]
    strings = locate(file.split(",", -1).map(_.trim))
    if (strings.nonEmpty) {
      val path = strings.mkString(",")
      logger.info(s"loading data from $path")
      loadRDD = sc.textFile(path)
    } else {
      if (nullable) {
        loadRDD = sc.emptyRDD[String]
        logger.warn(s"No data found in path '$file', enable null, created empty rdd.")
      } else {
        throw new Exception(s"No data exist in path '$file'.")
      }
    }
    loadRDD
  }

  /**
   * 判断paths下是否为空
   *
   * @param paths 要判断的paths
   * @param sc    SparkContext
   * @return 不为空的路径
   */
  def locate(paths: Array[String])(implicit sc: SparkContext): Array[String] = withFS(fs => {
    paths.flatMap(p => {
      val s = fs.globStatus(p)
      val checked = if (s == null || s.isEmpty) {
        logger.warn(s"Path not exist '$p'.")
        Array.empty[String]
      } else if (isFile(p)) {
        Array(p)
      } else {
        s.map(e => {
          val path = e.getPath
          // path存在，并且 path 为文件 或 path 为空目录，返回 path
          val finalPath = if (isFile(path) || 0 == getSize(path)) path.toString
          else {
            // path 为非空目录
            new Path(path, "*").toString
          }
          finalPath
        })
      }
      logger.info(s"Found file from '$p' -> '${checked.mkString(",")}'.")
      checked
    })
  })

  /**
   * 判断目录是否为 file
   *
   * @param path path
   * @param sc   SparkContext
   * @return
   */
  def isFile(path: String)(implicit sc: SparkContext): Boolean = withFS(fs => fs.isFile(new Path(path)))

  /**
   * 获取文件大小
   *
   * @param path path
   * @param sc   SparkContext
   * @return
   */
  def getSize(path: String)(implicit sc: SparkContext): Long = withFS(fs => fs.getContentSummary(path).getLength())
}
