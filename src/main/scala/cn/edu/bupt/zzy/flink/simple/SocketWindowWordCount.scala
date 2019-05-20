package cn.edu.bupt.zzy.flink.simple

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    // 要连接的端口
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 通过连接到socket获取输入数据
    val text: DataStream[String] = env.socketTextStream("hadoop000", port, '\n')

    // 解析数据，分组，设置窗口，聚合得到数量
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // 用单线程打印结果，而不是以并行的方式
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // 给有数量的单词一个数据类型
  case class WordWithCount(word: String, count: Long)
}
