package com.executivesummary.ExecutiveSummaryReport

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

object transposeDF {
  def main(args : Array[String]) {

    val spark = SparkSession.builder()
      .appName("dftranspose").master("local").getOrCreate()

    //val sqlContext = new HiveContext(spark.sparkContext)
    val sqlContext = spark.sqlContext

    val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
        .load("C:\\Users\\Shariva\\Desktop\\Naveens Task\\stats.csv")
      //.load("hdfs:///user/prabodhan/data/stats.csv")
    df.show()


    df.createOrReplaceTempView("transpose")

    sqlContext.sql("select collect_list(InvoiceNumber) as cola, collect_list(CurrentGrossCharges) as colb,collect_list(DiscountsPromotionsFees) as colc, collect_list(Taxes) as cold, collect_list(CurrentNetCharges) as cole , collect_list(PreviousBalance) as colf, collect_list(PaymentsReceived) as colg, collect_list(Adjustments) as colh, collect_list(TotalBalance) as coli from transpose").createOrReplaceTempView("tablesplitter")

    sqlContext.sql("select concat_ws(':',cola) as cola_new,concat_ws(':',colb) as colb_new,concat_ws(':',colc) as colc_new, concat_ws(':',cold) as cold_new, concat_ws(':',cole) as cole_new, concat_ws(':',colf) as colf_new, concat_ws(':',colg) as colg_new, concat_ws(':',colh) as colh_new , concat_ws(':',coli) as coli_new from tablesplitter").createOrReplaceTempView("tablesplitter_new")

    //sqlContext.sql("select cola_new, colb_new,colc_new,cold_new,cole_new from tablesplitter_new").show()

    sqlContext.sql("""select split(cola_new,':') [0] as col1,split(cola_new,':') [1] as col2,split(cola_new,':') [2] as col3,split(cola_new,':') [3] as col4 from tablesplitter_new union all
 select split(colb_new,':') [0] as col1,split(colb_new,':') [1] as col2,split(colb_new,':') [2] as col3,split(colb_new,':') [3] as col4 from tablesplitter_new union all select split(colc_new,':') [0] as col1,split(colc_new,':') [1] as col2,split(colc_new,':') [2] as col3,split(colc_new,':') [3] as col4 from tablesplitter_new union all select split(cold_new,':') [0] as col1,split(cold_new,':') [1] as col2,split(cold_new,':') [2] as col3,split(cold_new,':') [3] as col4 from tablesplitter_new union all select split(cole_new,':') [0] as col1,split(cole_new,':') [1] as col2,split(cole_new,':') [2] as col3,split(cole_new,':') [3] as col4  from tablesplitter_new union all select split(colf_new,':') [0] as col1,split(colf_new,':') [1] as col2,split(colf_new,':') [2] as col3,split(colf_new,':') [3] as col4  from tablesplitter_new union all select split(colg_new,':') [0] as col1,split(colg_new,':') [1] as col2,split(colg_new,':') [2] as col3,split(colg_new,':') [3] as col4  from tablesplitter_new union all select split(colh_new,':') [0] as col1,split(colh_new,':') [1] as col2,split(colh_new,':') [2] as col3,split(colh_new,':') [3] as col4  from tablesplitter_new union all select split(coli_new,':') [0] as col1,split(coli_new,':') [1] as col2,split(coli_new,':') [2] as col3,split(coli_new,':') [3] as col4  from tablesplitter_new""").createOrReplaceTempView("finaltable")

    //sqlContext.sql("select * from finaltable").show()

    val tmpfinaldf = sqlContext.sql("select row_number() over (order by 1)  as rnk,col1 from finaltable")
    tmpfinaldf.show()


    val manualschema = new StructType(Array(StructField("Data",StringType,true)))
    val dummyRows = Seq(Row("InvoiceNumber"),Row("CurrentGrossCharges"),Row("DiscountsPromotionsFees"),Row("Taxes"),Row("CurrentNetCharges"), Row("PreviousBalance"),Row("PaymentsReceived"),Row("Adjustments"),Row("TotalBalance"))
    val myRDD = spark.sparkContext.parallelize(dummyRows)
    spark.createDataFrame(myRDD,manualschema).createOrReplaceTempView("secondfinaltable")

    val tmpsecondfinaldf = sqlContext.sql("select row_number() over (order by 1)  as rnk,Data from secondfinaltable")
    tmpsecondfinaldf.show()

    val joinexpr = tmpfinaldf.col("rnk")===tmpsecondfinaldf.col("rnk")
    tmpfinaldf.join(tmpsecondfinaldf,joinexpr).createOrReplaceTempView("mergetable")


    sqlContext.sql("select Data,col1 as Value from mergetable").show()
  }
}