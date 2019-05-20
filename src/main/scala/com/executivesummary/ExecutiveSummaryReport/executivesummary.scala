package com.executivesummary.ExecutiveSummaryReport

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException

object executivesummary {
  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder()
    .appName("executivesummary").master("local")
    .getOrCreate()
    
    val sqlContext = spark.sqlContext
    
    import sqlContext.implicits._
   
     val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
     .load("file:///C:/Users/Shariva/Desktop/Naveens Task/stats.csv")
     df.show()
    
     df.createOrReplaceTempView("transpose")
   
   sqlContext.sql("select collect_list(InvoiceNumber) as cola, collect_list(CurrentGrossCharges) as colb,collect_list(DiscountsPromotionsFees) as colc, collect_list(Taxes) as cold, collect_list(CurrentNetCharges) as cole , collect_list(PreviousBalance) as colf, collect_list(PaymentsReceived) as colg, collect_list(Adjustments) as colh, collect_list(TotalBalance) as coli from transpose").createOrReplaceTempView("tablesplitter")
   
   sqlContext.sql("select concat_ws(':',cola) as cola_new,concat_ws(':',colb) as colb_new,concat_ws(':',colc) as colc_new, concat_ws(':',cold) as cold_new, concat_ws(':',cole) as cole_new, concat_ws(':',colf) as colf_new, concat_ws(':',colg) as colg_new, concat_ws(':',colh) as colh_new , concat_ws(':',coli) as coli_new from tablesplitter").createOrReplaceTempView("tablesplitter_new")
 
 //sqlContext.sql("select cola_new, colb_new,colc_new,cold_new,cole_new from tablesplitter_new").show()
 
    sqlContext.sql("""select split(cola_new,':') [0] as col1,split(cola_new,':') [1] as col2,split(cola_new,':') [2] as col3,split(cola_new,':') [3] as col4 from tablesplitter_new union all
     select split(colb_new,':') [0] as col1,split(colb_new,':') [1] as col2,split(colb_new,':') [2] as col3,split(colb_new,':') [3] as col4 from tablesplitter_new union all select split(colc_new,':') [0] as col1,split(colc_new,':') [1] as col2,split(colc_new,':') [2] as col3,split(colc_new,':') [3] as col4 from tablesplitter_new union all select split(cold_new,':') [0] as col1,split(cold_new,':') [1] as col2,split(cold_new,':') [2] as col3,split(cold_new,':') [3] as col4 from tablesplitter_new union all select split(cole_new,':') [0] as col1,split(cole_new,':') [1] as col2,split(cole_new,':') [2] as col3,split(cole_new,':') [3] as col4  from tablesplitter_new union all select split(colf_new,':') [0] as col1,split(colf_new,':') [1] as col2,split(colf_new,':') [2] as col3,split(colf_new,':') [3] as col4  from tablesplitter_new union all select split(colg_new,':') [0] as col1,split(colg_new,':') [1] as col2,split(colg_new,':') [2] as col3,split(colg_new,':') [3] as col4  from tablesplitter_new union all select split(colh_new,':') [0] as col1,split(colh_new,':') [1] as col2,split(colh_new,':') [2] as col3,split(colh_new,':') [3] as col4  from tablesplitter_new union all select split(coli_new,':') [0] as col1,split(coli_new,':') [1] as col2,split(coli_new,':') [2] as col3,split(coli_new,':') [3] as col4  from tablesplitter_new""").createOrReplaceTempView("finaltable")
 
 
  val tmpfinaldf = sqlContext.sql("select row_number() over (order by 1)  as rnk,col1 from finaltable")
  tmpfinaldf.show()
  
 
  val transposeschema = new StructType(Array(StructField("Data",StringType,true)))
  val dummyRows = Seq(Row("InvoiceNumber"),Row("CurrentGrossCharges"),Row("DiscountsPromotionsFees"),Row("Taxes"),Row("CurrentNetCharges"), Row("PreviousBalance"),Row("PaymentsReceived"),Row("Adjustments"),Row("TotalBalance"))
  val myRDD = spark.sparkContext.parallelize(dummyRows)
  spark.createDataFrame(myRDD,transposeschema).createOrReplaceTempView("secondfinaltable")
  
  val tmpsecondfinaldf = sqlContext.sql("select row_number() over (order by 1)  as rnk,Data from secondfinaltable")
  tmpsecondfinaldf.show()
  
  val joinexpr = tmpfinaldf.col("rnk")===tmpsecondfinaldf.col("rnk")
 tmpfinaldf.join(tmpsecondfinaldf,joinexpr).createOrReplaceTempView("mergetable")

  
  val transposedf = sqlContext.sql("select Data,col1 as Value from mergetable")
  transposedf.show()
  
  
 
 val manualschema = new StructType(Array(StructField("Description",StringType,true), StructField("Calls",IntegerType,true), StructField("MinSec",StringType,true), StructField("Charges",DoubleType,true)))
 
//Outbound
val outbounddf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/outbound.csv")
outbounddf.show()

val outboundrdd = outbounddf.rdd

outboundrdd.collect.foreach(x => 
  {  
    val dummyRows = Seq(Row("Outbound",null,null,null),Row("Domestic Switched",x(1),x(2),x(3)),Row("Domestic Dedicated",x(4),x(5),x(6)),Row("International(including Canada & Mexico",x(7),x(8),x(9))); 
    val mytmprdd = spark.sparkContext.parallelize(dummyRows); 
    spark.createDataFrame(mytmprdd,manualschema).createOrReplaceTempView("outboundtable");
  })

//Inbound
val indf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/inbound.csv")
indf.show()

val inrdd = indf.rdd

inrdd.collect.foreach(x => { println(x); val dummyRows = Seq(Row("Inbound",null,null,null),Row("Domestic Switched",x(1),x(2),x(3)),Row("Domestic Dedicated",x(4),x(5),x(6)),Row("International(including Canada & Mexico",x(7),x(8),x(9))); val mytmprdd = spark.sparkContext.parallelize(dummyRows); spark.createDataFrame(mytmprdd,manualschema).createOrReplaceTempView("inboundtable");
})

//Directory Assistance
val ddf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/directoryassistance.csv")
ddf.show()

val drdd = ddf.rdd

drdd.collect.foreach(x => { println(x); val dummyRows = Seq(Row("Directory Assistance",null,null,null),Row("Directory Assistance",x(1),x(2),x(3))); val mytmprdd = spark.sparkContext.parallelize(dummyRows); spark.createDataFrame(mytmprdd,manualschema).createOrReplaceTempView("dstable");
})


//Calling card
val ccdf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/callingcard.csv")
ccdf.show()

val ccrdd = ccdf.rdd

ccrdd.collect.foreach(x => { println(x); val dummyRows = Seq(Row("Calling Card",null,null,null),Row("Domestic",x(1),x(2),x(3)),Row("International(including Canada & Mexico)",x(4),x(5),x(6)),Row("Directory Assistance",x(7),x(8),x(9))); val mytmprdd = spark.sparkContext.parallelize(dummyRows); spark.createDataFrame(mytmprdd,manualschema).createOrReplaceTempView("cctable");
})

//Ring to
val rtdf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/ringto.csv")
rtdf.show()

val rtrdd = rtdf.rdd

rtrdd.collect.foreach(x => { println(x); val dummyRows = Seq(Row("Ring To",null,null,null),Row("Domestic Switched",x(1),x(2),x(3)),Row("Domestic Dedicated",x(4),x(5),x(6)),Row("International(including Canada & Mexico",x(7),x(8),x(9))); val mytmprdd = spark.sparkContext.parallelize(dummyRows); spark.createDataFrame(mytmprdd,manualschema).createOrReplaceTempView("rttable");
})


val finaldf = sqlContext.sql("select Description, Calls, MinSec,Charges From outboundtable union all select Description, Calls, MinSec,Charges From inboundtable union all select Description, Calls, MinSec,Charges From dstable union all select Description, Calls, MinSec,Charges From cctable union all select Description, Calls, MinSec,Charges From rttable")
finaldf.show()



//Non-Usage

val manualschemafornonusage = new StructType(Array(StructField("Description",StringType,true), StructField("NonrecurringCharges",DoubleType,true), StructField("MonthlyCharges",DoubleType,true)))
 
val nudf = spark.read.format("csv").option("header","true").option("inferSchema","true")
.load("file:///C:/Users/Shariva/Desktop/Naveens Task/nonusage.csv")
nudf.show()

val nurdd = nudf.rdd

nurdd.collect.foreach(x => { println(x); val dummyRows = Seq(Row("Non-Usages",null,null),Row("Dedicated Services",x(1),x(2)),Row("Equipment/Other Charges",x(3),x(4)),Row("Dial Services",x(5),x(6)),Row("VPN Services",x(7),x(8))); val mytmprdd = spark.sparkContext.parallelize(dummyRows); spark.createDataFrame(mytmprdd,manualschemafornonusage).createOrReplaceTempView("nonusagetable");
})


val nonusagedf = sqlContext.sql("select Description, NonrecurringCharges, MonthlyCharges from nonusagetable")
nonusagedf.show()


//Merging 3 dataframes
//val mergedf = transposedf.unionAll( finaldf).unionAll(nonusagedf)
//mergedf.show(100)


//mergedf.write.option("header", "true").csv("hdfs:///user/prabodhan/data/out")

    transposedf.repartition(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true")
    .save("file:///D:/data/executivesummary")
    finaldf.repartition(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true")
    .save("file:///D:/data/executivesummary")
    nonusagedf.repartition(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true")
    .save("file:///D:/data/executivesummary")
    
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val srcdirpath = new Path("file:///D:/data/executivesummary")
    val destfilepath = new Path("file:///D:/data/out/executivesummary.csv")
    
    mergefiles(fs,srcdirpath,fs,destfilepath,false,conf)
  }
  
  def mergefiles( srcFS: FileSystem, srcDir: Path,
    dstFS: FileSystem, dstFile: Path,
    deleteSource: Boolean, conf: Configuration) : Boolean = {
    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")
    println("merging starts...")
    if(srcFS.getFileStatus(srcDir).isDirectory()){
      val outputFile = dstFS.create(dstFile)
      
      Try {
        srcFS.listStatus(srcDir)
        .sortBy(_.getPath.getName)
        .collect {
          case status if status.isFile() => 
            val inputFile = srcFS.open(status.getPath())
            Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
            inputFile.close()
        }
      }
      outputFile.close()
      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else
      false
  }
}