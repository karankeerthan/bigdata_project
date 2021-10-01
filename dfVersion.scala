import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object dfVersion extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

//-----------------------------------------------------------------------------------------
// load customers
  val custD = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://ms.itversity.com:3306/retail_db")
  .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "customers")
  .option("user", "retail_user")
  .option("password", "itversity")
  .load().persist(StorageLevel.MEMORY_AND_DISK)
  
  val custDF = custD.repartition(2)
  custDF.createOrReplaceTempView("customers") 
  custDF.printSchema
 
//-----------------------------------------------------------------------------------------
// load orders
  
   val orderD = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://ms.itversity.com:3306/retail_db")
   .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "orders")
  .option("user", "retail_user")
  .option("password", "itversity")
  .load().persist(StorageLevel.MEMORY_AND_DISK)
  
   val ordDF = orderD.repartition(2)
   ordDF.createOrReplaceTempView("orders")
   ordDF.printSchema()
//----------------------------------------------------------------------------------------
// user input
   
  println("Type first_name : ")
  val fname = scala.io.StdIn.readLine()
  println("Type last_name : ")
  val lname = scala.io.StdIn.readLine()

//-----------------------------------------------------------------------------------------
// inner join
  
   val dfJoin = ordDF.join(custDF, ordDF("order_customer_id") === custDF("customer_id"))
   dfJoin.createOrReplaceTempView("joinTable")
   
//-----------------------------------------------------------------------------------------
// left anti join
   
   val leftJoin = custDF.join(ordDF, custDF("customer_id") === ordDF("order_customer_id"),"leftanti")
   leftJoin.createOrReplaceTempView("lJoin")
   
//-----------------------------------------------------------------------------------------
// query a
   
   val DF1 = custDF.select("*").where(s"customer_fname == '$fname' and customer_lname == '$lname'").show()
   
//-----------------------------------------------------------------------------------------
// query b
   
   val DF2 = ordDF.selectExpr("order_status","month(order_date) as month").groupBy("order_status","month").count().show()
   
//-----------------------------------------------------------------------------------------
// query c
   
    val DF3 = dfJoin.selectExpr("order_status","month(order_date) as month")
    .where(s"customer_fname == '$fname' and customer_lname == '$lname'")
    .groupBy("order_status","month").count().show()
   
//-----------------------------------------------------------------------------------------
// query d
   
    val DF4 = dfJoin.selectExpr("customer_id","order_status")
    .groupBy("customer_id","order_status").count()
    
//-----------------------------------------------------------------------------------------
// query e
   
    val DF5 = dfJoin.select("customer_id","customer_fname","customer_lname").distinct
    
//-----------------------------------------------------------------------------------------    
// query f
   
    val DF6 = leftJoin.selectExpr("customer_id","customer_fname","customer_lname").distinct.orderBy("customer_id")

 //-----------------------------------------------------------------------------------------    
// query g
    
   import spark.implicits._
   val DF7 = dfJoin.selectExpr("customer_id ").groupBy("customer_id").count().sort($"count".desc).limit(5)
 
//------------------------------------------------------------------------------------------
// query h
    
   val DF8 = dfJoin.selectExpr("customer_id","order_date").orderBy("order_date")
   
//-----------------------------------------------------------------------------------------    
// query j
   
 val DF10a = dfJoin.selectExpr("customer_fname", "customer_lname", "order_status").where(s"customer_fname == '$fname' and customer_lname == '$lname' and order_status like 'CLOSED'")
  .groupBy("order_status","customer_fname", "customer_lname").count()
  
 val DF10b = dfJoin.selectExpr("customer_fname", "customer_lname", "order_status").where(s"customer_fname == '$fname' and customer_lname == '$lname' and order_status not like 'CLOSED'")
  .groupBy("order_status","customer_fname", "customer_lname").count()
 
//-----------------------------------------------------------------------------------------     
// query k

  val DF11 = custDF.selectExpr("customer_state").groupBy("customer_state").count()
 
   
  spark.stop()
}