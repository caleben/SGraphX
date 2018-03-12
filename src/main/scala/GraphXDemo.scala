/**
  * @author wenci
  * @date: 2018/3/6
  * @description: GraphX构建图（由顶点和边组成），运用API做一些图计算以及实现pageRank算法的运用
  *
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx.GraphLoader._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXDemo {
  def main(args: Array[String]): Unit = {
    //解决：Could not locate executable null\bin\winutils.exe in the Hadoop binaries
    System.setProperty("hadoop.home.dir","F:\\javatools\\dependency_jar\\winutils")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    //设置顶点和边，它们都是用元组定义的Array
    //顶点 VD:(String, Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边 ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //构造图Graph[VD, ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    println("属性演示")
    println("**********************************************************")

  /*  //基本属性操作
    //num操作
    println(s"顶点个数: ${graph.numVertices}, 边个数: ${graph.numEdges}")
    println
    //degrees操作
    println(s"图中各顶点的度数:\n${graph.degrees.collect.mkString("\n")}\n出度:\n${graph.outDegrees.collect.mkString("\n")}\n入度:\n${graph.inDegrees.collect.mkString("\n")}")
    println
    //顶点，边， 三属性操作
    println(s"顶点:\n${graph.vertices.collect.mkString("\n")}\n边:\n${graph.edges.collect.mkString("\n")}\n三属性:\n${graph.triplets.collect.mkString("\n")}")
    println

    //顶点操作
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
      v => println(s"${v._1} is ${v._2}")
    }
    println

    //边操作
    println("找出图中大于5的边")
    graph.edges.filter{e => e.attr > 5}.collect.foreach{
      e => println(s"${e.srcId} to ${e.dstId} attr ${e.attr}")
    }
    println

    //triplets操作, triplets包括两个顶点和一条边
    println("列出边属性大于5的triplets")
    for(triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} like ${triplet.dstAttr._1}")
    }
    println

    //Degrees操作，
    println("找出图中最大的出度，入度，度数")
    def max(a:(VertexId, Int), b:(VertexId, Int)): (VertexId, Int) = {
      if(a._2 > b._2) a else b
    }
    println(s"max of outDegrees: ${graph.outDegrees.reduce(max)}, max of inDegrees: ${graph.inDegrees.reduce(max)}, max of Degrees: ${graph.degrees.reduce(max)}")
    println

    //转换操作
    println("转换操作")
    println("顶点的转换操作：顶点age+10")
    graph.mapVertices{ case(id,(name,age)) => (id,(name,age+10)) }.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println("边的转换操作：边属性*2")
    graph.mapEdges{ e => e.attr*2}.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} attr ${e.attr}"))
    println

    //结构操作，子图
    println("顶点年纪大于30的子图")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30 )//vpred与epred是此方法的两个匿名函数
    //val suGraph = graph.subgraph(epred = e => e.dstId>e.srcId )
    println("子图所有顶点")
    subGraph.vertices.collect.foreach(s => println(s"${s._1} is ${s._2}"))
    println("子图所有边")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} attr ${e.attr}"))
    println
    //将图反转reverse
    println(s"将图反转: ${graph.reverse.triplets.collect.mkString("\n")}")
    println

    //连接操作,Graphx的一个核心操作
    println("连接操作")
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0)}
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees){
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees){
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
    println("连接图的属性")
    userGraph.vertices.collect.foreach(v => println(s"${v._1} is ${v._2.name}, inDegrees: ${v._2.inDeg}, outDegrees: ${v._2.outDeg}"))
    println("出度与入度相同的人员")
    userGraph.vertices.filter(v => v._2.inDeg == v._2.outDeg).foreach(v => println(s"${v._2.name}"))
    println

    //实例运用，找出5到各顶点的最短路线
    println("找出5到各顶点的最短")
    val sourceId: VertexId = 5L
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {//计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )*/
//    println(sssp.vertices.collect.mkString("\n"))
//    println(sssp.edges.collect.mkString("\n"))

    //PageRank算法的运用
    val pageRankGraph = GraphLoader.edgeListFile(sc, "F:\\javatools\\myProjects\\graphxDemo\\src\\main\\resources\\followers.txt")
    val ranks = pageRankGraph.pageRank(0.001).vertices
    val users = sc.textFile("F:\\javatools\\myProjects\\graphxDemo\\src\\main\\resources\\users.txt").map{
      line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    
    println(ranksByUsername.collect.mkString("\n"))
    ;
    sc.stop()
  }
}
