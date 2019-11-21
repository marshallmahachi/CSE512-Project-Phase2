package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectangle = queryRectangle.split(",")
      val point = pointString.split(",")

      var returnBool = false
      var rect_x1 = rectangle(0).toDouble
      var rect_y1 = rectangle(1).toDouble

      var rect_x2 = rectangle(2).toDouble
      var rect_y2 = rectangle(3).toDouble

      var point_x = point(0).toDouble
      var point_y = point(1).toDouble

      var lower_bound_x: Double = rect_x1
      var upper_bound_x: Double = rect_x1

      var lower_bound_y: Double = rect_y1
      var upper_bound_y: Double = rect_y1


      if (rect_x1 > rect_x2){
        lower_bound_x = rect_x2
        upper_bound_x = rect_x1
      } else {
        lower_bound_x = rect_x1
        upper_bound_x = rect_x2
      }

      if (rect_y1 > rect_y2){
        lower_bound_y = rect_y2
        upper_bound_y = rect_y1
      } else {
        lower_bound_y = rect_y1
        upper_bound_y = rect_y2
      }

      //then run the condition now
      if (point_x >= lower_bound_x && point_x <= upper_bound_x && point_y >= lower_bound_y && point_y <= upper_bound_y){
        returnBool = true
      }

      returnBool

    }))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //todo: will need to refactor the code & define the function outside rangeQuery & rangeJoinQuery
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>({
      val rectangle = queryRectangle.split(",")
      val point = pointString.split(",")

      var returnBool = false
      var rect_x1 = rectangle(0).toDouble
      var rect_y1 = rectangle(1).toDouble

      var rect_x2 = rectangle(2).toDouble
      var rect_y2 = rectangle(3).toDouble

      var point_x = point(0).toDouble
      var point_y = point(1).toDouble

      var lower_bound_x: Double = rect_x1
      var upper_bound_x: Double = rect_x1

      var lower_bound_y: Double = rect_y1
      var upper_bound_y: Double = rect_y1


      if (rect_x1 > rect_x2){
        lower_bound_x = rect_x2
        upper_bound_x = rect_x1
      } else {
        lower_bound_x = rect_x1
        upper_bound_x = rect_x2
      }

      if (rect_y1 > rect_y2){
        lower_bound_y = rect_y2
        upper_bound_y = rect_y1
      } else {
        lower_bound_y = rect_y1
        upper_bound_y = rect_y2
      }

      //then run the condition now
      if (point_x >= lower_bound_x && point_x <= upper_bound_x && point_y >= lower_bound_y && point_y <= upper_bound_y){
        returnBool = true
      }

      returnBool

    }))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
