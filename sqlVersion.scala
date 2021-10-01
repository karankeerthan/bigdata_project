import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object customers extends App{
  
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
// outer join
   
   val outJoin = ordDF.join(custDF, ordDF("order_customer_id") === custDF("customer_id"),"outer")
   dfJoin.createOrReplaceTempView("outJoin")
   
//-----------------------------------------------------------------------------------------
// left anti join
   
   val leftJoin = custDF.join(ordDF, custDF("customer_id") === ordDF("order_customer_id"),"leftanti")
   leftJoin.createOrReplaceTempView("lJoin")
   
//-----------------------------------------------------------------------------------------
//  Query a
   
  val query = s"select * from customers where customer_fname like '$fname' and customer_lname like '$lname'"
  val sqlDF = spark.sql(query).show()
  
//-----------------------------------------------------------------------------------------
//  Query b
  
  val sqlDF2 = spark.sql("""select month(order_date) as byMonth,order_status,count(*) as count from orders
    group by month(order_date),order_status 
    order by month(order_date)""").show(100,false)
    
//-----------------------------------------------------------------------------------------
//  Query c
    
    val sqlDF3 = spark.sql("""select month(order_date) as byMonth,order_status,count(*) as count from joinTable
    where customer_fname like 'Ann' and customer_lname like 'Smith'
    group by month(order_date),order_status 
    order by month(order_date)""").show(100,false)

//-----------------------------------------------------------------------------------------
//  Query d
    
    val sqlDF4 = spark.sql("""select customer_id,order_status,count(*) as count from joinTable
    group by customer_id,order_status """).show(100,false)

//-----------------------------------------------------------------------------------------
//  Query e
    
    val sqlDF5 = spark.sql("""select distinct(customer_id),customer_lname,customer_fname from joinTable
     """).show(100,false)
  
//-----------------------------------------------------------------------------------------
//  Query f
     
       val sqlDF6  = spark.sql("""select distinct(customer_id),customer_lname,customer_fname from lJoin 
         order by customer_id""").show(100,false)
         
//-----------------------------------------------------------------------------------------
         
       val newJoin = dfJoin.coalesce(1) 
       newJoin.createOrReplaceTempView("newjoinTable")
       
//------------------------------------------------------------------------------------------
//  Query g
       
      val sqlDF7  = spark.sql("""select  customer_id, customer_lname, customer_fname, count(*) as Tot from newjoinTable
     group by customer_id,customer_lname, customer_fname
     order by Tot desc
     limit 5
       """).show(100,false)
       
//-----------------------------------------------------------------------------------------
//  Query h
       
   val sqlDF8 = spark.sql("""select customer_id, customer_lname, customer_fname, order_date from newjoinTable
     order by order_date asc
     """).show(100,false)
     
//-----------------------------------------------------------------------------------------
//  Query i
     
     val sqlDF9 = spark.sql(""" select coalesce(max(o.order_date),'0000-00-00 00:00:00') as last_order_date, c.customer_id
       from orders as o right join customers as c on o.order_customer_id = c.customer_id
       group by c.customer_id
       order by c.customer_id
       """).show(100,false)
     
//-----------------------------------------------------------------------------------------
//  Query j
    
     val sqlDF10 = spark.sql(s"""select customer_fname, customer_lname, order_status,count(*) as count from newjoinTable
    where customer_fname like '$fname' and customer_lname like '$lname' and
    order_status like 'CLOSED'
    group by order_status,customer_fname, customer_lname """).show(100,false)
    
    val sqlDF10a = spark.sql(s"""select customer_fname, customer_lname,order_status,count(*) as count from newjoinTable
    where customer_fname like '$fname' and customer_lname like '$lname' and
    order_status not like 'CLOSED'
    group by order_status,customer_fname, customer_lname """).show(100,false)

//-----------------------------------------------------------------------------------------
//  Query k
    
    val sqlDF11 = spark.sql("""select customer_state, count(*) as stateCount from customers
      group by customer_state
      """).show(100,false)
      

  spark.stop()
}