import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel


object req extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  //-----------------------------------------------------------------------------------------
 // load customers
  val custDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://ms.itversity.com:3306/retail_db")
  .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "customers")
  .option("user", "retail_user")
  .option("password", "itversity")
  .load().persist(StorageLevel.MEMORY_AND_DISK)
  
  custDF.createOrReplaceTempView("customers") 
 //custDF.printSchema
 
 //-----------------------------------------------------------------------------------------
 // load orders
  
   val ordDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://ms.itversity.com:3306/retail_db")
   .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "orders")
  .option("user", "retail_user")
  .option("password", "itversity")
  .load().persist(StorageLevel.MEMORY_AND_DISK)
  
   ordDF.createOrReplaceTempView("orders")
 //ordDF.printSchema
//-----------------------------------------------------------------------------------------
// inner join
   
   val dfJoin = ordDF.join(custDF, ordDF("order_customer_id") === custDF("customer_id"))
   dfJoin.createOrReplaceTempView("joinTable")
   
//-----------------------------------------------------------------------------------------
// find the customers and all their details who are from the city "Caguas"
    val rq1 = spark.sql("""select *  from joinTable
      where customer_city like "Caguas"
    """)
    
    val df1 = dfJoin.select("*").where("customer_city like 'Caguas'").show()
    
//-----------------------------------------------------------------------------------------
// find the customers and all their details who are from the city "Caguas"
    val rq2 = spark.sql("""select *  from joinTable
      where order_status like "COMPLETE"
    """)
    
     val df2 = dfJoin.select("*").where("order_status like 'COMPLETE'").show()

}