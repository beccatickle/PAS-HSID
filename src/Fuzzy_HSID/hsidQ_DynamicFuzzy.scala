package Fuzzy_HSID

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.math._
import scala.util.control.Breaks._

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object hsidQ_DynamicFuzzy {

  /* fuzzy version with dynamic hot spot membership functions */

  val DELIMITER = "," // split data at this point
  val PERCENT_INIT = 10

  // indexes for updated datasets
  val INDEX_LATITUDE = 1
  val INDEX_LONGITUDE = 2
  val INDEX_BEARING = 3
  val INDEX_HS_AGE = 4 // in original data, this is vehicle make => not needed so replacing with age of hotspot (number of batches has existed for)
  val INDEX_PARTIAL_LOCATION = 5
  val INDEX_FULL_LOCATION = 6
  val INDEX_DATE = 7
  val INDEX_TIME = 8
  val INDEX_WEEKDAY = 9

  val INCIDENT_WEIGHT = 1.0 // amount to add to hot spot fitness per new incident
  //  val DECAY_RATE = 0.3 // amount to decay fitness by each interval
  //  val DELETE_THRESHOLD = 1.9 // hot spots with fitness < DELETE_THRESHOLD will be deleted
  //  val HOTSPOT_THRESHOLD = 5.0 // hot spots are only actually hot spots with fitness > HOTSPOT_THRESHOLD
  //  val HOTSPOT_MILEAGE = 5.0 // radius of hotspots

  //val CONFIDENCE_TH = 0.2 // desired minimum strength of membership of incident to hot spot for incident to start to impact hs
  val DEF_STANDARD_DEV = 1 // standard deviation for gaussian mf, default value to use if some issue with the hotspots own value
  val MAX_SD = 2.75 // max sd allowed
  val MIN_SD = 0.15 // min sd allowed
  val DENSE_TH = 1.45 // half way between max and min SD

  // labels to use for dense and sparse hot spots
  val DENSE_LABEL = "DENSE"
  val SPARSE_LABEL = "SPARSE"
  val INIT_LABEL = "NONE" // used when hs initialised and has no given label yet

  // k values of densities
  val D_VL = 0.1 // very low
  val D_L = 0.35 // low
  val D_M = 0.55 // medium
  val D_H = 0.75 // high
  val D_VH = 1.0 // very high

  // indexes of mfs in mf arrays
  val INDEX_LOW = 0
  val INDEX_MED = 1
  val INDEX_HIGH = 2

  type HotspotKey = (Double, Double) // latitude, longitude
  type HotspotInfoArr = Array[String]
  type FuzzyInfo = (Double, String) // (standard deviation for m.f, HS label)
  type SumVars = (Int, Double) // (number of instances added, total distance of instances added)
  type FVal = Double
  type HotspotVal = (FVal, HotspotInfoArr, FuzzyInfo, SumVars) // fitness value, info array, fuzzy info, temporary totals
  type Hotspot = (HotspotKey, HotspotVal)

  type DensityRule = (Int, Int, Double) // NumInsts, AvgDist, Density


  def main(arg: Array[String]) {

    // set level of logs
    Logger.getLogger("org").setLevel(Level.OFF)

    // get parameters
    val logger = Logger.getLogger(this.getClass)
    if (arg.length < 8) {
      logger.error("=> wrong parameters number")
      System.err.println("Parameters \n\t<Batch interval> \n\t<Decay rate>\n\t<Delete threshold>\n\t<Hotspot threshold>\n\t<Mileage>\n\t<Partitions>\n\t<Input file>\n\t<Output dir>\n\t<Num Batches>\n\t<Execution time (OPTIONAL)>")
      System.exit(1)
    }

    val batchInterval = arg(0).toInt
    val decayRate = arg(1).toDouble
    val deleteThreshold = arg(2).toDouble
    val hotspotThreshold = arg(3).toDouble
    val maxMileage = arg(4).toDouble
    val initMileage = arg(5).toDouble
    val confTh = arg(6).toDouble
    val numPartitions = arg(7).toInt
    val inputFile = arg(8)
    val outputDir = arg(9)
    val numBatches = arg(10).toInt

    val jobName = "HSID-"+outputDir

    var timeout = -1
    if(arg.length > 11) {
      timeout = arg(11).toInt
    }

    /* set up fuzzy rules array */

    // array of rules (NumInsts, AvgDist, Density) --> all rules of format IF a AND b THEN c

    // INITIAL RULE BASE
    /*val r0 = (INDEX_LOW, INDEX_LOW, D_M)
    val r1 = (INDEX_MED, INDEX_LOW, D_H)
    val r2 = (INDEX_HIGH, INDEX_LOW, D_VH)
    val r3 = (INDEX_LOW, INDEX_MED, D_L)
    val r4 = (INDEX_MED, INDEX_MED, D_M)
    val r5 = (INDEX_HIGH, INDEX_MED, D_H)
    val r6 = (INDEX_LOW, INDEX_HIGH, D_VL)
    val r7 = (INDEX_MED, INDEX_HIGH, D_L)
    val r8 = (INDEX_HIGH, INDEX_HIGH, D_M)*/

    // ALTERNATIVE RULE BASE - tighten requirements for high density hot spots
    val r0 = (INDEX_LOW, INDEX_LOW, D_M)
    val r1 = (INDEX_MED, INDEX_LOW, D_H)
    val r2 = (INDEX_HIGH, INDEX_LOW, D_VH)
    val r3 = (INDEX_LOW, INDEX_MED, D_VL)
    val r4 = (INDEX_MED, INDEX_MED, D_L)
    val r5 = (INDEX_HIGH, INDEX_MED, D_L)
    val r6 = (INDEX_LOW, INDEX_HIGH, D_VL)
    val r7 = (INDEX_MED, INDEX_HIGH, D_VL)
    val r8 = (INDEX_HIGH, INDEX_HIGH, D_L)

    val arr_rules: Array[DensityRule] = Array(r1, r2, r3, r4, r5, r6, r7, r8)

    /* end rules array */

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    // set up initial hot spot cache
    // we need to manually maintain this as built-in state doesn't allow access to keys without first updating the state
    var hotspotCache: RDD[Hotspot] = sc.emptyRDD

    // Create a StreamingContext from SparkContext
    // Batch interval: specifies how often to process new data
    val ssc = new StreamingContext(sc, Seconds(batchInterval))


    /***********************************************/

    val rddQueue = new Queue[RDD[Array[String]]]()

    /***********************************************/


    val rawIncidentStream: InputDStream[Array[String]] = ssc.queueStream(rddQueue,
      oneAtATime = true)

    val listen = new MyJobListener(ssc, numBatches)
    ssc.addStreamingListener(listen)

    // set checkpoint directory - required for stateful functions
    // will need to use this to prevent DAG growing too much
    // checkpoint example here http://www.spark.tc/stateful-spark-streaming-using-transform/
    //ssc.checkpoint("./checkpoints") // checkpointing turned off for now to stop error =>

//    val incidentSplitStream: DStream[Array[String]] = rawIncidentStream.map(line => line.split(DELIMITER))

    // change to (key,value) DStream where key = (lat,lng)
    val incidentLocationStream: DStream[(HotspotKey, HotspotInfoArr)] = rawIncidentStream.repartition(numPartitions).map(incident => {
      val incidentWithDays = incident
      incidentWithDays(INDEX_HS_AGE) = "0" // replace vehicle make with age of incident - start at 0 because we increment it later
      ((incident(INDEX_LATITUDE).toDouble, incident(INDEX_LONGITUDE).toDouble), incident)
    })

    // operate on DStream through individual RDDs using transform
    // this transform first allocates incidents to existing hot spots
    // then any incidents left form new hot spots amongst themselves w.r.t hot spot mileage and other constraints
    val actualHotspots: DStream[Hotspot] = incidentLocationStream.transform(rdd => {

      val startTime = System.nanoTime()

      println("\n----------------------------------------------------------------------------------")
      println("\nStart partitions: "+rdd.partitions.size)
      println("\nmaxMileage: "+maxMileage)
      println("\ninitMileage: "+initMileage)
      println("\nnumber of incidents: " + rdd.count())

      val beforeRedByHS = System.nanoTime()

      /******************** If no hotspots exist yet, we do prestage1 *******************************/

      val currentHScount: Long = hotspotCache.count()
      println("HS/INCIDENTS >>>>>>> "+currentHScount+" // "+rdd.count())

      var hotspotBroadcast = sc.broadcast(new Array[Hotspot](0))
      var initialHotspotsRDD: RDD[Hotspot] = sc.emptyRDD
      var leftoverIncidents: RDD[(HotspotKey, HotspotInfoArr)] = sc.emptyRDD

      if(currentHScount == 0L) {
        println("\tNo hotspots in state")

        // remove initial suppressor set from set of incidents
        val rawIncidentsIndexes = rdd.zipWithIndex().persist()
        val initialNumSuppressors = rawIncidentsIndexes.count() * PERCENT_INIT / 100
        val suppressorSetIndexes: Array[((HotspotKey, HotspotInfoArr), Long)] = rawIncidentsIndexes.takeSample(false, initialNumSuppressors.toInt)
        println("\tsuppressor set initialised with this number: "+suppressorSetIndexes.size)
        val indexesSelected = suppressorSetIndexes.map(line => line._2).toSet
        val leftover: RDD[(HotspotKey, HotspotInfoArr)] = rawIncidentsIndexes.filter{case (key, value) => !indexesSelected.contains(value)}.map(line => line._1).persist()

        // remove redundancies within set of initial hotspots
        val suppressorSet: Array[Hotspot] = suppressorSetIndexes.map(line => (line._1._1, (1.0, line._1._2, (-1.0,INIT_LABEL), (0, 0.0)))) // where 1 represents number of incidents part of this HS (JUST 1 TO START WITH)- centre has max membership of 1
        val initialHotspots: Array[Hotspot] = selfReduceFuzzy(suppressorSet, maxMileage, initMileage, confTh)
              .map(hs => (hs._1, (hs._2._1.toDouble, hs._2._2, hs._2._3, hs._2._4))) // convert fitness to Double
        println("\t initial number of hotspots: "+initialHotspots.size)

        hotspotBroadcast = sc.broadcast(initialHotspots)
        leftoverIncidents = leftover
        initialHotspotsRDD = sc.parallelize(initialHotspots).persist()

      } else {
        hotspotBroadcast = sc.broadcast(hotspotCache.collect())
        leftoverIncidents = rdd
      }

      /****************************** End prestage1 *********************************************/

      println("\nbroadcast val size >>>>>> "+hotspotBroadcast.value.size)
      println("check initial HS rdd >>>>>> "+initialHotspotsRDD.count())
      println("check leftover incidents >>>>> "+leftoverIncidents.count())

      // allocate new incidents to hot spots in cache
      // use coalesce() to reduce number of partitions => performance improvement
      // changed to use repartition(4) because using files as input results in 1 partition at the start
      val reducedWithHotspotsRDD = leftoverIncidents.mapPartitionsWithIndex((index, incidents) =>
        reduceWithBroadcastHS(incidents, hotspotBroadcast, index, confTh, maxMileage)) // STAGE 1

      println("PARTITIONS AFTER COALESCE: "+reducedWithHotspotsRDD.partitions.size)

      println("\nnumber after reduceWithHotspots: " + reducedWithHotspotsRDD.count())
      val timeForRedFunc = (System.nanoTime - beforeRedByHS)/ 1e9
      val timeBeforeByKey = System.nanoTime()

      // accumulate number of incidents per hotspot
      // add number of incidents (and aggregate SumVars) and maintain boolean (true = possible new hotspot, false = assigned to existing hotspot)
      val countIncidentsRDD: RDD[(HotspotKey, (FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean))] = reducedWithHotspotsRDD
        .reduceByKey((a,b) => (a._1 + b._1, a._2, a._3, (a._4._1 + b._4._1, a._4._2 + b._4._2), a._5))
      println("\nnumber after reduceWithHotspots - BY KEY: " + countIncidentsRDD.count())

      val timeForRedWithHS = (System.nanoTime - beforeRedByHS)/ 1e9
      val timeForByKey = (System.nanoTime - timeBeforeByKey)/ 1e9
      println("\nTime for reduce function only: " + timeForRedFunc)
      println("\nTime for By Key only: " + timeForByKey)
      println("\nTotal time for STAGE 1: " + timeForRedWithHS)

      // filter countIncidentsRDD => split into left over incidents and incidents already allocated to hotspots
      val incidentsForNewHotspots: RDD[Hotspot] = countIncidentsRDD
        .filter(incident => incident._2._5)
        .map(incident => (incident._1, (incident._2._1, incident._2._2, incident._2._3, incident._2._4)))
      val incidentsForExistingHotspots: RDD[Hotspot] = countIncidentsRDD
        .filter(incident => !incident._2._5)
        .map(incident => (incident._1, (incident._2._1, incident._2._2, incident._2._3, incident._2._4)))

      println("\nincidentsForNewHotspots: " + incidentsForNewHotspots.count())
      println("\nincidentsForExistingHotspots: " + incidentsForExistingHotspots.count())

      val beforeRedNew = System.nanoTime()

      // remove redundancies within incidentsForNewHotspots => first collect back to driver and operate on there
      // incidents should be relatively small as we have already removed any incidents that can be allocated to existing hotspots
      val incidentsArr: Array[Hotspot] = incidentsForNewHotspots.collect()
      val reducedIncidentsNew = selfReduceFuzzy(incidentsArr, maxMileage, initMileage, confTh) // STAGE 2
      val parallelIncidentsNew: RDD[Hotspot] = sc.parallelize(reducedIncidentsNew, numPartitions) // check what this does with partitioning

      val added = parallelIncidentsNew.count()
      println("\n we are adding this many (candidate) hotspots to the state: "+added)

      // join together RDD of incident allocations to existing hot spots with RDD of incidents that have created new hot spots
      val allHotspotUpdates: RDD[Hotspot] = incidentsForExistingHotspots.union(parallelIncidentsNew)

      val timeForRedNew = (System.nanoTime - beforeRedNew)/ 1e9
      println("\nTime for STAGE 2: " + timeForRedNew)

      // what we return is the coordinates of all hot spots that should be updated/added to the state,
      // along with the corresponding number of incidents they incorporate
      //allHotspotUpdates

      /* *************************************************************************************************************** */
      // merged everything into one transform in order to access timings

      val beforeStage3 = System.nanoTime()

      // corresponds to n*INCIDENT_WEIGHT part of fitness update formula
      val weightedUpdates: RDD[Hotspot] = allHotspotUpdates
        .map(update => { // n*INCIDENT_WEIGHT and increment hotspot age
          val detailsArr: HotspotInfoArr = update._2._2
          detailsArr(INDEX_HS_AGE) = (detailsArr(INDEX_HS_AGE).toInt + 1).toString
          (update._1, (update._2._1.toDouble*INCIDENT_WEIGHT, detailsArr, update._2._3, update._2._4))
        })
        .union(initialHotspotsRDD
          .map(hs => { // n*INCIDENT_WEIGHT and increment hotspot age
            val detailsArr: HotspotInfoArr = hs._2._2
            detailsArr(INDEX_HS_AGE) = (/*detailsArr(INDEX_HS_AGE).toInt +*/ 1).toString
            (hs._1, (hs._2._1.toDouble*INCIDENT_WEIGHT, detailsArr, hs._2._3, hs._2._4))
        }))

      // includes decay of fitness of all existing hotspots => corresponds to oldFitness*(1-DECAY_RATE) part of fitness formula
      val unionEverything = weightedUpdates
        .union(hotspotCache
          .map(hotspot => { // decay fitness, and increment hotspot age
            val detailsArr: HotspotInfoArr = hotspot._2._2
            detailsArr(INDEX_HS_AGE) = (detailsArr(INDEX_HS_AGE).toInt + 1).toString
            (hotspot._1, (hotspot._2._1*(1.0-decayRate),detailsArr, hotspot._2._3, hotspot._2._4))
          }))
        .coalesce(numPartitions) // coalesce added to stop accumulation of partitions in state RDD over time

      val timeBeforeFinalRed = System.nanoTime()

      // aggregate fitness and SumVars for hotspots, then remove hotspots with fitness < deletion threshold
      val newStateBeforeDelete: RDD[Hotspot] = unionEverything
        .reduceByKey((a, b) => ((a._1 + b._1), a._2, a._3, ((a._4._1 + b._4._1),(a._4._2 + b._4._2))))

      val beforeDelete = newStateBeforeDelete.count()

      // separated deletion out just so that we can calculate how many are deleted each time
      val newState: RDD[Hotspot] = newStateBeforeDelete.filter(hotspot => (hotspot._2._1 >= deleteThreshold))

      val newStateCount = newState.count()

      val deleted = beforeDelete - newStateCount
      println("\n we have removed this many hotspots (< DELETE_THRESHOLD): "+deleted)

      println("\nnew state: " + newStateCount)
      val timeForFinalRed = (System.nanoTime - timeBeforeFinalRed)/ 1e9
      println("\nTime for final reduce: "+timeForFinalRed)

      val timeBeforeCache = System.nanoTime()

      /* FUZZY M.F. UPDATE */

      val newStateFuzzyUpdated: RDD[Hotspot] = newState.map(h => performFuzzyUpdate(h, arr_rules, decayRate))

      /* End fuzzy m.f. update */

      hotspotCache.unpersist() // to prevent it taking up memory
      hotspotCache = newStateFuzzyUpdated // update cache
      hotspotCache.cache // cache new state, so can access in next streaming batch

      println("\nhotspot cache count: "+hotspotCache.count())
      val timeForCache = (System.nanoTime - timeBeforeCache)/ 1e9
      println("\nTime for cache: "+timeForCache)

      /* calc fitness value stats */

      // calculate range of fitness values
      val justFitnessValues: RDD[Double] = newStateFuzzyUpdated.map(hs => hs._2._1)
      val FV_max = justFitnessValues.max()

      val justFVsorted: RDD[Double] = justFitnessValues.sortBy(x => x, true, justFitnessValues.partitions.size)
      val numFVs: Long = justFVsorted.count()
      val fvWithIndex = justFVsorted.zipWithIndex().map(x => (x._2, x._1))

      val indexOf10th = (0.1*numFVs).toLong
      val indexOf25th = (0.25*numFVs).toLong
      val indexOf50th = (0.5*numFVs).toLong
      val indexOf75th = (0.75*numFVs).toLong
      val indexOf90th = (0.90*numFVs).toLong
      val indexOf99th = (0.99*numFVs).toLong

      val fv10: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf10th).collect()
      val fv25: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf25th).collect()
      val fv50: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf50th).collect()
      val fv75: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf75th).collect() // for now we are using the 75th percentile
      val fv90: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf90th).collect()
      val fv99: Array[(Long, Double)] = fvWithIndex.filter(x => x._1 == indexOf99th).collect()


      /* end calc fv stats */

      /* calc sDev stats */

      // calculate range of fitness values
      val justSDevValues: RDD[Double] = newStateFuzzyUpdated.map(hs => hs._2._3._1)
      val SDev_max = justSDevValues.max()

      val justSDsorted: RDD[Double] = justSDevValues.sortBy(x => x, true, justSDevValues.partitions.size)
      val numSDs: Long = justSDsorted.count()
      val sdWithIndex = justSDsorted.zipWithIndex().map(x => (x._2, x._1))

      val indexOf10thSD = (0.1*numSDs).toLong
      val indexOf25thSD = (0.25*numSDs).toLong
      val indexOf50thSD = (0.5*numSDs).toLong
      val indexOf75thSD = (0.75*numSDs).toLong
      val indexOf90thSD = (0.90*numSDs).toLong
      val indexOf99thSD = (0.99*numSDs).toLong

      val sd10: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf10thSD).collect()
      val sd25: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf25thSD).collect()
      val sd50: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf50thSD).collect()
      val sd75: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf75thSD).collect()
      val sd90: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf90thSD).collect()
      val sd99: Array[(Long, Double)] = sdWithIndex.filter(x => x._1 == indexOf99thSD).collect()


      /* end calc sDEV stats */


      // filter hotspots => only want to output those that have a fitness > HOTSPOT_THRESHOLD
      val actualHotspots = newStateFuzzyUpdated.filter(hotspot => hotspot._2._1 >= hotspotThreshold)
      val actualHotspotsCount = actualHotspots.count()
      println("\n Actual Hotspots: " + actualHotspotsCount)

      val sparseHS = actualHotspots.filter(h => h._2._3._2 == SPARSE_LABEL)
      val denseHS = actualHotspots.filter(h => h._2._3._2 == DENSE_LABEL)

      val nSparse = sparseHS.count()
      val nDense = denseHS.count()

      val stage3Time = (System.nanoTime - beforeStage3)/ 1e9
      println("\nSTAGE 3 TIME: "+stage3Time)

      val runtime = (System.nanoTime - startTime)/ 1e9
      println("\nRUNTIME: "+runtime)

      var streamOutput = "" //+ newStateCount + "\t" + actualHotspotsCount + "\t" + runtime + "\n"

      streamOutput += "************ RESULTS **************"

      streamOutput += "\nSTAGE 1 TIME: "+timeForRedWithHS
      streamOutput += "\nSTAGE 2 TIME: "+timeForRedNew
      streamOutput += "\nSTAGE 3 TIME: "+stage3Time
      streamOutput += "\nTOTAL TIME: "+ runtime +"\n"
      streamOutput += "\nHOTSPOTS DELETED: "+deleted
      streamOutput += "\nHOTSPOTS ADDED: "+added
      streamOutput += "\n> DELETE_THRESHOLD: "+newStateCount
      streamOutput += "\n> HOTSPOT_THRESHOLD: "+actualHotspotsCount

      streamOutput += "\n\nDENSE HS: "+nDense
      streamOutput += "\nSPARSE HS: "+nSparse

      streamOutput += "\n\nFV_10: "+"%.2f".format(fv10(0)._2)
      streamOutput += "\nFV_25: "+"%.2f".format(fv25(0)._2)
      streamOutput += "\nFV_50: "+"%.2f".format(fv50(0)._2)
      streamOutput += "\nFV_75: "+"%.2f".format(fv75(0)._2)
      streamOutput += "\nFV_90: "+"%.2f".format(fv90(0)._2)
      streamOutput += "\nFV_99: "+"%.2f".format(fv99(0)._2)
      streamOutput += "\nFV_MAX: "+"%.2f".format(FV_max)
      streamOutput += "\nFV_MIN: "+"%.2f".format(justFitnessValues.min())+"\n"

      streamOutput += "\n\nSD_10: "+"%.2f".format(sd10(0)._2)
      streamOutput += "\nSD_25: "+"%.2f".format(sd25(0)._2)
      streamOutput += "\nSD_50: "+"%.2f".format(sd50(0)._2)
      streamOutput += "\nSD_75: "+"%.2f".format(sd75(0)._2)
      streamOutput += "\nSD_90: "+"%.2f".format(sd90(0)._2)
      streamOutput += "\nSD_99: "+"%.2f".format(sd99(0)._2)
      streamOutput += "\nSD_MAX: "+"%.2f".format(SDev_max)
      streamOutput += "\nSD_MIN: "+"%.2f".format(justSDevValues.min())+"\n"

      streamOutput += "\n************   END   **************"

      println(streamOutput)

      var directory = new File("./"+ outputDir + "/algorithmDetails/")

      if(!directory.exists()){
        println("\nDIR DOES NOT EXIST")
        directory.mkdirs()
      }

      var pw = new PrintWriter(new File("./"+ outputDir + "/algorithmDetails/interval_" + System.nanoTime() ))

      //var pw = new PrintWriter(new File("./_OUTPUT/interval_.txt"))
      pw.write(streamOutput)
      pw.close()

      // return actual hotspots so we can output them to file
      actualHotspots
    })

    // format output as strings: latitude longitude fitness age // sDev, density label, tempInstCount (should be 0 for all)
    val hotspotStrings: DStream[String] = actualHotspots
      .map(hotspot => hotspot._1._1 + "\t" + hotspot._1._2 + "\t" + hotspot._2._1 + "\t" + hotspot._2._2(INDEX_HS_AGE) + "\t" + hotspot._2._3._1 + "\t" + hotspot._2._3._2 + "\t" + hotspot._2._4._1)

    // save as text file
    //hotspotStrings.saveAsTextFiles(outputDir + "/hotspotDetails/interval") // saves output in dir prefix-TIME

    hotspotStrings.foreachRDD(rdd => { // overwrites previous output each batch

      val collectedHS = rdd.collect()

      var directory = new File("./"+ outputDir + "/hotspotDetails/")

      if(!directory.exists()){
        println("\nDIR DOES NOT EXIST")
        directory.mkdirs()
      }

      var outStr = ""
      for(i <- 0 until collectedHS.size) {
        outStr += collectedHS(i) + "\n"
      }

      var pw1 = new PrintWriter(new File("./"+ outputDir + "/hotspotDetails/interval_" + System.nanoTime() ))

      pw1.write(outStr)
      pw1.close()

    })


    hotspotStrings.print()

    /********************************************************************************************/

    ssc.start() // to start receiving data, call start() on the streaming context
    if(timeout == -1) {
      println("NO TIMEOUT")


        for(i <- 1 to numBatches) {
          rddQueue.synchronized{
            val inputI = inputFile + "_" + i + ".csv" //"HC_batch_"+i+".csv"
            val thisRDD: RDD[String] = sc.textFile(inputI, numPartitions)
            val splitRDD: RDD[Array[String]] = thisRDD.map(line => line.split(DELIMITER)).cache()
            rddQueue+= splitRDD.cache()
          }

      }

       while(listen.getNBatches()<numBatches) Thread.sleep(1000)

       ssc.stop()

    } else {
      println("USING TIMEOUT - "+timeout)
      ssc.awaitTerminationOrTimeout(timeout)
    }


  }

  /** Returns distance between incident and hot spot, in miles
    *
    * @param incident (lat,lng) location of incident
    * @param hotspot (lat,lng) location of hot spot
    */
  def getMiles(incident: (HotspotKey, HotspotInfoArr), hotspot: (HotspotKey, HotspotInfoArr)): Double = {

    var miles = -1.0

    if (incident._2(INDEX_PARTIAL_LOCATION).equalsIgnoreCase(hotspot._2(INDEX_PARTIAL_LOCATION))) { // don't bother comparing if not on the same road

      // check bearing
      val bearing_diff = scala.math.abs(incident._2(INDEX_BEARING).toInt - hotspot._2(INDEX_BEARING).toInt)
      val module = (bearing_diff + 180) % 360 - 180
      val delta = scala.math.abs(module)

      if (delta < 60) { // if bearing ok, calculate haversine distance between incident and hotspot
        miles = Haversine.haversine(incident._1._1, incident._1._2, hotspot._1._1, hotspot._1._2)
      }
    }

    miles
  }

  /** Calculates firing strength of a rule based on AND connective
    *
    * @param numI number of instances (x)
    * @param avgD average distance (y)
    * @param ant1 id of mf of antecedent 1
    * @param ant2 id of mf of antecedent 2
    * @return
    */
  def calcW(numI: Int, avgD: Double, ant1: Int, ant2: Int): Double = {

    // calc membership in ant1 (insts)
    var mAnt1: Double = 0.0
    if(ant1 == INDEX_LOW) { // low
      val b = 0.0
      val c = 3.0
      mAnt1 = math.max(((c-numI)/(c-b)),0)
    } else if(ant1 == INDEX_MED) { // medium
      val a = 0.0
      val b = 3.0
      val c = 8.0
      mAnt1 = math.max(math.min(((numI-a)/(b-a)),((c-numI)/(c-b))),0)
    } else if(numI>=10) { // high, with numI > 10
      mAnt1 = 1.0
    } else { // high, using standard triangular membership func
      val a = 3.0
      val b = 8.0
      mAnt1 = math.max(((numI-a)/(b-a)),0)
    }

    // calc membership in ant2 (dist)
    var mAnt2: Double = 0.0
    if(ant2 == INDEX_LOW) { // low
      val b = 0.0
      val c = 0.75
      mAnt2 = math.max(((c-avgD)/(c-b)),0)
    } else if(ant2 == INDEX_MED) { // medium
      val a = 0.25
      val b = 1.0
      val c = 1.75
      mAnt2 = math.max(math.min(((avgD-a)/(b-a)),((c-avgD)/(c-b))),0)
    } else if(avgD>=2.0) { // high, with avgD > 2
      mAnt2 = 1.0
    } else { // high, using standard triangular membership func
      val a = 1.25
      val b = 2.0
      mAnt2 = math.max(((avgD-a)/(b-a)),0)
    }

    // find minimum (because AND)
    val w: Double = math.min(mAnt1, mAnt2)

    w

  }

  /**Calculates density proportion of a hotspot given the number of instances added to it and the average distance of the insts from the centre
    *
    * @param numI number of instances
    * @param avgD average distance to centre
    * @return
    */
  def calcDensProportion(numI: Int, avgD: Double, rules: Array[DensityRule]): Double = {

    var sumWK = 0.0
    var sumW = 0.0

    // for each rule, calculate w (firing strength)
    for(i <- 0 until rules.size) {

      val weight = calcW(numI, avgD, rules(i)._1, rules(i)._2)

      sumW += weight
      sumWK += (weight*rules(i)._3)

    }

    // combine outputs of rules

    val output: Double = sumWK / sumW // density proportion

    output

  }

  def performFuzzyUpdate(h: Hotspot, rules: Array[DensityRule], dr: Double): Hotspot = {

    val numInsts: Int = h._2._4._1

    var newSD: Double = DEF_STANDARD_DEV

    if(numInsts > 0) { // at least one incident has been added to the hot spot in this time interval

      /* Do standard update --> Fuzzy Inf to get density proportion, then calculate SD from that */

      val avgDist: Double = h._2._4._2 / numInsts.toDouble // calculate avg dist for instances added to h

      var densityProportion: Double = calcDensProportion(numInsts, avgDist, rules) // calculate density proportion of new instances

      if(densityProportion < 0.01) {
        densityProportion = 0.01
      } else if(densityProportion > 1.0) {
        densityProportion = 1.0
      }

      val sdRange: Double = MAX_SD - MIN_SD
      val sdNewInsts: Double = MIN_SD + ((1.0-densityProportion)*sdRange) // calculate standard dev wrt new instances
      val oldSD: Double = h._2._3._1

      // if hs just created --> just use density of new insts, otherwise use weighted average
      newSD = if(oldSD==(-1.0)) sdNewInsts else (((0.5*oldSD)+sdNewInsts) / 1.5) // newSD = weighted average of old sd (w=0.5) and newInstSD (w=1)

    } else { // no instances have been added to the hot spot this interval

      /* 'decay' SD wrt decay rate --> dense hot spots will eventually decay to sparse hot spots */

      val oldSD: Double = h._2._3._1
      var sdTemp: Double = oldSD * (1.0+dr)

      if(sdTemp > MAX_SD) {
        sdTemp = MAX_SD
      }
      newSD = sdTemp

    }

    // more dense hs have a lower standard dev
    val newLabel: String = if(newSD<DENSE_TH) DENSE_LABEL else SPARSE_LABEL

    val newSDrounded: Double = (newSD*100).round / 100.toDouble // round to 2dp
    val newFuzzyInfo: FuzzyInfo = (newSDrounded, newLabel)
    val newH: Hotspot = (h._1, (h._2._1, h._2._2, newFuzzyInfo, (0,0.0)))

    newH

  }

  /** Removes redundancies from within a single set of incidents
    *
    * @param incidents Array of incidents to reduce
    * @param maxMileage fixed possible maximum mileage of a hot spot
    * @param initialisationMileage mileage to form initial hot spots with
    *
    * @return Array of hot spot centres, with their associated fitness value and info array
    */
  def selfReduceFuzzy(incidents: Array[Hotspot], maxMileage: Double, initialisationMileage: Double, confTh: Double): Array[Hotspot] = {

    val initialReduction: Array[Hotspot] = selfReduce(incidents, initialisationMileage)

    println("SelfReduceFuzzy: originalIncCount = "+incidents.size)
    println("SelfReduceFuzzy: after reduction = "+initialReduction.size)

    initialReduction

  }

  /** Removes redundancies within a set of incidents - the non-fuzzy version
    * Collects RDD back to driver so must be small set of incidents
    *
    * @param incidents Incidents to reduce
    * @param mileage Radius of hot spots
    *
    * @return Array of identified hot spot centres, their associated fitness value and info arrays
    */
  def selfReduce(incidents: Array[Hotspot], mileage: Double): Array[Hotspot] = {

    // incidents should be relatively small as we have already removed any incidents that can be allocated to existing hotspots
    val incidentsArr: Array[Hotspot] = incidents
    val centres = mutable.HashMap.empty[HotspotKey, HotspotVal] // this will store the resulting hotspot centres and their fitnesses

    for(i <- 0 until incidentsArr.size) { // i = incident to allocate

      if(!centres.contains(incidentsArr(i)._1)) { // if incident is already a centre, don't bother comparing it to anything => once it is a centre, cannot be allocated to another hot spot
        var closestIncident: Option[(Int,Double)] = None // Option[(index of closest incident,miles)]

        breakable {
          for(j <- 0 until incidentsArr.size ) { // compare each incident against every other incident
            if(i != j) { // don't bother comparing against itself

              // compare location of i and j => if i is close enough to j, allocate i to j => j becomes hotspot centre
              // only compare if i < j OR (i > j AND j corresponds to an already determined centre)
              // if j < i and is not a centre, then it has been already allocated to another centre, and therefore is effectively 'removed' => should not compare against it
              if(j > i || (j < i && centres.contains(incidentsArr(j)._1))) {

                val miles = getMiles((incidentsArr(i)._1, incidentsArr(i)._2._2), (incidentsArr(j)._1, incidentsArr(j)._2._2)) // calculate Haversine distance between incidents
                // Not taking bearing into account yet

                // un-nest these if statements at some point...
                if(miles != -1.0) { // miles == -1.0 would mean hot spot and incident were not on the same road, or bearing is preventing them being combined

                  if(miles <= mileage) { // incident i is close enough to incident j that i could be allocated to j

                    if(closestIncident.isEmpty) { // not yet found hotspot close enough
                      closestIncident = Some((j,miles))
                    } else if(miles < closestIncident.get._2) { // incident j is closer than previously found closest incident
                      closestIncident = Some((j,miles))
                    }
                  }
                }

              }

            }

            // if found a neighbour, we can break out of the loop
            if(!closestIncident.isEmpty) {
              break
            }
          }
        }

        // if closestIncident is still None, then no incident was found close enough
        //    => keep incident's original (lat,lng) and fitness, and add it as a centre in its own right
        // otherwise
        //    => if closestIncident is already in centres, update its fitness with incident i's fitness
        //    => else, add closestIncident to centres, and then update its fitness with incident i's fitness
        if(closestIncident.isEmpty) {

          // update temporary SumVars to reflect singular incident with distance 0
          val origInc = incidentsArr(i)
          val newCentre = (origInc._1, (origInc._2._1, origInc._2._2, origInc._2._3, (1, 0.0)))
          centres += newCentre // add incident as centre in its own right

        } else if(centres.contains(incidentsArr(closestIncident.get._1)._1)) { // closest incident already in centres

          val m = closestIncident.get._2 // miles
          val closestIndex = closestIncident.get._1
          val centreLocation: (Double, Double) = incidentsArr(closestIndex)._1 // key in HashMap
          val newFitness: FVal = centres(centreLocation)._1 + incidentsArr(i)._2._1
          val newSumIncs = centres(centreLocation)._4._1 + 1 // increment sum of incidents added
          val newSumDist = centres(centreLocation)._4._2 + m // add to sum of distances
          val replaceCentre = (centreLocation,(newFitness, centres(centreLocation)._2, centres(centreLocation)._3, (newSumIncs, newSumDist)))
          centres += replaceCentre // replace the previously existing instance of key in HashMap

        } else { // closest incident not yet in centres

          val m = closestIncident.get._2 // miles
          val closestIndex = closestIncident.get._1
          val centreLocation: (Double, Double) = incidentsArr(closestIndex)._1
          val newFitness: FVal = incidentsArr(closestIndex)._2._1 + incidentsArr(i)._2._1
          val newSumIncs = 2 // one for centre itself, one for the reduced incident
          val newSumDist = m
          val newCentre = (centreLocation,(newFitness, incidentsArr(closestIndex)._2._2, incidentsArr(closestIndex)._2._3, (newSumIncs, newSumDist)))
          centres += newCentre // add centre to HashMap
        }

      }

    }

    // at this point, centres stores all the hotspot centres within the new incidents, as well as their fitnesses
    // convert to array so that we can parallelize() it, then return
    val centresArr: Array[Hotspot] = centres.toArray
    centresArr

  }

  /** Reduces a set of incidents wrt a set of hot spots (fuzzy version)
    *
    * @param iter Iterator containing incidents to be reduced
    * @param hs Array of hot spots
    * @param maxMileage maxmimum mileage allowed for any hot spot
    * @param confidenceTh How certain we want to be that incident is part of hot spot, limit on how small membership values are allowed
    *
    * @return An iterator containing the information to update the hot spots, along with a Boolean which is true if the
    *         incident was NOT reduced by any hot spot and FALSE if this element represents the update info for a hot spot
    *         The return iterator contains hot spot keys and how much to increase FV by for hot spot updates, and incident
    *         keys for those incidents unable to be reduced
    */
  def reduceWithHotspotsFuzzy(iter: Iterator[(HotspotKey, HotspotInfoArr)], hs: Array[Hotspot], maxMileage: Double, confidenceTh: Double): Iterator[(HotspotKey,(FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean))] = {

    //print("\nreduceWithHsFuzzy nonzero memberships: ")

    val updateInfo = mutable.HashMap.empty[HotspotKey, (FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean)] // here FVal is how much to increase the fitness of the hot spot by

    while(iter.hasNext) {

      val inc = iter.next()
      var notReduced = true

      for(h <- 0 until hs.size) {

        val formatH = (hs(h)._1, hs(h)._2._2) // format hot spot for getMiles function
        val miles: Double = getMiles(inc, formatH) // distance between inc and hot spot
        val sd: Double = hs(h)._2._3._1 // standard deviation for this hot spot

        val membership = calcMembershipToHS(inc, hs(h), miles, maxMileage, sd)

//        if(membership > 0) {
//          print(membership + ",")
//        }

        if(membership >= confidenceTh) {

          notReduced = false

          val hKey: HotspotKey = hs(h)._1

          if(updateInfo.contains(hKey)) { // if h already in hashmap --> keep same data, with FV+=m and updated SumVals

            val newFitUpdate: FVal = updateInfo(hKey)._1 + membership
            val newSumIncs: Int = updateInfo(hKey)._4._1 + 1
            val newSumDist: Double = updateInfo(hKey)._4._2 + miles
            val newUpdateElem: (HotspotKey, (FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean)) = (hKey, (newFitUpdate, updateInfo(hKey)._2, updateInfo(hKey)._3, (newSumIncs, newSumDist), notReduced))
            updateInfo += newUpdateElem // replace hs in hashmap

          } else { // if hot spot update info not yet in hashmap

            val newUpdateElem: (HotspotKey, (FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean)) = (hKey, (membership, hs(h)._2._2, hs(h)._2._3, (1, miles), notReduced))
            updateInfo += newUpdateElem // add hs to hashmap

          }

        }

      }

      if(notReduced) { // if incident not reduced by an hot spot, initialise with hs initial values and flag as notReduced

        val newUpdateElem: (HotspotKey, (FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean)) = (inc._1, (1.0, inc._2, (-1, INIT_LABEL), (0,0.0), notReduced))
        updateInfo += newUpdateElem // add incident to hashmap

      }

    }

    println("\nUpdate Hashmap Size: "+updateInfo.size)

    val result = updateInfo.toIterator
    result

  }

  /** Calculates membership of an incident to a given hot spot
    *
    * @param incident The incident to calculate membership for
    * @param hotspot The fuzzy hot spot
    * @param distIH result of getMiles() between inc and hotspot
    * @param maxMileage maximum radius allowed for any hot spots
    * @param standDev standard deviation to use for gaussian mf of this hs
    * @return A double representing the membership of incident in hotspot
    */
  def calcMembershipToHS(incident: (HotspotKey, HotspotInfoArr), hotspot: Hotspot, distIH: Double, maxMileage: Double, standDev: Double): Double = {

    // distIH --> if -1 then membership = 0, if >maxmilage then = 0, other wise calculate membership wrt the number of Miles

    // check for issues with the standardDeviation and replace with default if there are problems
    val sd = if (standDev>0) standDev else DEF_STANDARD_DEV

    var membership: Double = 0.0

    if(distIH != -1.0 && distIH <= maxMileage) { // distIH will be -1 if the locations are on different roads or bearings

      // calc membership
      val gPower: Double = (-0.5*math.pow((distIH/standDev),2)) // centre = 0
      val m = math.exp(gPower)

      // round membership to 2dp
      val rounded: Double = (m*100).round / 100.toDouble

      membership = m

    }

    if(membership > 1.0) membership = 1.0

    membership

  }

  /** Extracts hot spots from broadcast variable, then calls reduceWithHotSpots
    *
    * @param iter iterator of incidents to reduce
    * @param hsBroadcast Broadcast variable, array of hotspots
    * @param index Index of the partition
    * @return An iterator containing the information to update the hot spots, along with a Boolean which is true if the
    *         incident was NOT reduced by any hot spot and FALSE if this element represents the update info for a hot spot
    *         The return iterator contains hot spot keys and how much to increase FV by for hot spot updates, and incident
    *         keys for those incidents unable to be reduced
    */
  def reduceWithBroadcastHS(iter: Iterator[(HotspotKey, HotspotInfoArr)],
                            hsBroadcast: Broadcast[Array[Hotspot]],
                            index: Int,
                            confidenceTh: Double,
                            maxMileage: Double): Iterator[(HotspotKey,(FVal, HotspotInfoArr, FuzzyInfo, SumVars, Boolean))] = {

    val hs: Array[Hotspot] = hsBroadcast.value // extract hotspots from broadcast variable

    val result = reduceWithHotspotsFuzzy(iter, hs, maxMileage, confidenceTh) // perform reduction
    result

  }

  // compute distance between GPS coordinates
  object Haversine {
    val R = 6372.8/1.609344  //radius in miles!!

    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians

      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    }

  }


  private class MyJobListener(ssc: StreamingContext, totalBatches: Long) extends StreamingListener {
    private var nBatches: Long = 0
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      if(nBatches < totalBatches)
        nBatches += 1
      else {
        println("**************************************************NUMBER OF BATCHES COMPLETED.")
        //ssc.stop(true)
      }

      println("\nNumber of batches processed: " + nBatches)
      val binfo = batchCompleted.batchInfo
      println("\nBatch scheduling delay: " + binfo.schedulingDelay.getOrElse(0L))
      println("\nBatch processing time: " + binfo.processingDelay.getOrElse(0L))
    }

    def getNBatches(): Int ={
      nBatches.toInt
    }
  }

}

