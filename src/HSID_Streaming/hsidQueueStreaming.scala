package HSID_Streaming

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import scala.collection.mutable.Queue

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object hsidQueueStreaming {

  val DELIMITER = "," // split data at this point
  val PERCENT_INIT = 10 // initial proportion of hot spots 

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
    val mileage = arg(4).toDouble
    val numPartitions = arg(5).toInt
    val inputFile = arg(6)
    val outputDir = arg(7)
    val numBatches = arg(8).toInt

    val jobName = "HSID-"+outputDir

    var timeout = -1
    if(arg.length > 9) {
      timeout = arg(9).toInt
    }

    val conf = new SparkConf().setAppName(jobName) 
    val sc = new SparkContext(conf)// create SparkContext first to allow us to broadcast variables

    // set up initial hot spot cache
    // we need to manually maintain this as built-in state doesn't allow access to keys without first updating the state
    var hotspotCache: RDD[((Double,Double), (Double, Array[String]))] = sc.emptyRDD

    // Create a StreamingContext from SparkContext
    // Batch interval: specifies how often to process new data
    val ssc = new StreamingContext(sc, Seconds(batchInterval))

     // input via fileStream - Create a DStream by Spark Streaming monitoring a given directory
  //  val rawIncidentStream: DStream[String] = ssc.textFileStream(inputDir)


    /***********************************************/
    // input via queueStream
    // load files into individual RDDs

    val rddQueue = new Queue[RDD[Array[String]]]()

    val rawIncidentStream: InputDStream[Array[String]] = ssc.queueStream(rddQueue,
      oneAtATime = true)

    /***********************************************/

  
    val listen = new MyJobListener(ssc, numBatches)
    ssc.addStreamingListener(listen)

//    val incidentSplitStream: DStream[Array[String]] = rawIncidentStream.map(line => line.split(DELIMITER))

    // change to (key,value) DStream where key = (lat,lng)
    val incidentLocationStream: DStream[((Double, Double), Array[String])] = rawIncidentStream.repartition(numPartitions).map(incident => {
      val incidentWithDays = incident
      incidentWithDays(INDEX_HS_AGE) = "0" // replace vehicle make with age of incident - start at 0 because we increment it later
      ((incident(INDEX_LATITUDE).toDouble, incident(INDEX_LONGITUDE).toDouble), incident)
    })

    // operate on DStream through individual RDDs using transform
    // this transform does stages 1 - 3 of the algorithm
    val actualHotspots: DStream[((Double, Double), (Double, Array[String]))] = incidentLocationStream.transform(rdd => {

      val startTime = System.nanoTime()

      println("\n----------------------------------------------------------------------------------")
      println("\nStart partitions: "+rdd.partitions.size)
      println("\nmileage: "+mileage)
      println("\nnumber of incidents: " + rdd.count())

      val beforeRedByHS = System.nanoTime()

      /******************** If no hotspots exist yet, we do prestage1 *******************************/

      val currentHScount: Long = hotspotCache.count()
      println("HS/INCIDENTS >>>>>>> "+currentHScount+" // "+rdd.count())

      var hotspotBroadcast = sc.broadcast(new Array[((Double, Double), (Double, Array[String]))](0))
      var initialHotspotsRDD: RDD[((Double, Double), (Double, Array[String]))] = sc.emptyRDD
      var leftoverIncidents: RDD[((Double, Double), Array[String])] = sc.emptyRDD

      if(currentHScount == 0L) {
        println("\tNo hotspots in state")

        // remove initial suppressor set from set of incidents
        val rawIncidentsIndexes = rdd.zipWithIndex().persist()
        val initialNumSuppressors = rawIncidentsIndexes.count() * PERCENT_INIT / 100
        val suppressorSetIndexes: Array[(((Double, Double), Array[String]), Long)] = rawIncidentsIndexes.takeSample(false, initialNumSuppressors.toInt)
        println("\tsuppressor set initialised with this number: "+suppressorSetIndexes.size)
        val indexesSelected = suppressorSetIndexes.map(line => line._2).toSet
        val leftover: RDD[((Double, Double), Array[String])] = rawIncidentsIndexes.filter{case (key, value) => !indexesSelected.contains(value)}.map(line => line._1).persist()

        // remove redundancies within set of initial hotspots
        val suppressorSet: Array[((Double, Double), (Int, Array[String]))] = suppressorSetIndexes.map(line => (line._1._1, (1, line._1._2)))
        val initialHotspots: Array[((Double, Double), (Double, Array[String]))] = reduceIncidentsNew(suppressorSet, mileage)
              .map(hs => (hs._1, (hs._2._1.toDouble, hs._2._2)))
        println("\t initial number of hotspots: "+initialHotspots.size)

        hotspotBroadcast = sc.broadcast(initialHotspots) // broadcast current state so can access it
        leftoverIncidents = leftover
        initialHotspotsRDD = sc.parallelize(initialHotspots).persist()

      } else {
        hotspotBroadcast = sc.broadcast(hotspotCache.collect()) // broadcast current state so can access it
        leftoverIncidents = rdd
      }

      /****************************** End prestage1 *********************************************/

      println("\nbroadcast val size >>>>>> "+hotspotBroadcast.value.size)
      println("check initial HS rdd >>>>>> "+initialHotspotsRDD.count())
      println("check leftover incidents >>>>> "+leftoverIncidents.count())

      // allocate new incidents to hot spots in cache
      val reducedWithHotspotsRDD = leftoverIncidents.mapPartitionsWithIndex((index, incidents) =>
        reduceWithHotspots(incidents, hotspotBroadcast, index, mileage)) // STAGE 1

      println("PARTITIONS: "+reducedWithHotspotsRDD.partitions.size)

      println("\nnumber after reduceWithHotspots: " + reducedWithHotspotsRDD.count())
      val timeForRedFunc = (System.nanoTime - beforeRedByHS)/ 1e9
      val timeBeforeByKey = System.nanoTime()

      // accumulate number of incidents per hotspot
      // add number of incidents and maintain boolean (true = possible new hotspot, false = assigned to existing hotspot)
      val countIncidentsRDD: RDD[((Double, Double), (Int, Array[String], Boolean))] = reducedWithHotspotsRDD
        .reduceByKey((a, b) => (a._1 + b._1, a._2, a._3))
      println("\nnumber after reduceWithHotspots - BY KEY: " + countIncidentsRDD.count())

      val timeForRedWithHS = (System.nanoTime - beforeRedByHS)/ 1e9
      val timeForByKey = (System.nanoTime - timeBeforeByKey)/ 1e9
      println("\nTime for reduce function only: " + timeForRedFunc)
      println("\nTime for By Key only: " + timeForByKey)
      println("\nTotal time for STAGE 1: " + timeForRedWithHS)

      // filter countIncidentsRDD => split into left over incidents and incidents already allocated to hotspots
      val incidentsForNewHotspots: RDD[((Double, Double), (Int, Array[String]))] = countIncidentsRDD
        .filter(incident => incident._2._3)
        .map(incident => (incident._1, (incident._2._1, incident._2._2)))
      val incidentsForExistingHotspots: RDD[((Double, Double), (Int, Array[String]))] = countIncidentsRDD
        .filter(incident => !incident._2._3)
        .map(incident => (incident._1, (incident._2._1, incident._2._2)))

      println("\nincidentsForNewHotspots: " + incidentsForNewHotspots.count())
      println("\nincidentsForExistingHotspots: " + incidentsForExistingHotspots.count())

      val beforeRedNew = System.nanoTime()

      // remove redundancies within incidentsForNewHotspots => first collect back to driver and operate on there
      // incidents should be relatively small as we have already removed any incidents that can be allocated to existing hotspots
      val incidentsArr: Array[((Double, Double), (Int, Array[String]))] = incidentsForNewHotspots.collect()
      val reducedIncidentsNew = reduceIncidentsNew(incidentsArr, mileage) // STAGE 2
      val parallelIncidentsNew: RDD[((Double, Double), (Int, Array[String]))] = sc.parallelize(reducedIncidentsNew, numPartitions)

      val added = parallelIncidentsNew.count()
      println("\n we are adding this many (candidate) hotspots to the state: "+added)

      // join together RDD of incident allocations to existing hot spots with RDD of incidents that have created new hot spots
      val allHotspotUpdates: RDD[((Double, Double), (Int, Array[String]))] = incidentsForExistingHotspots.union(parallelIncidentsNew)

      val timeForRedNew = (System.nanoTime - beforeRedNew)/ 1e9
      println("\nTime for STAGE 2: " + timeForRedNew)

      val beforeStage3 = System.nanoTime()

      // start of stage 3
      val weightedUpdates: RDD[((Double, Double), (Double, Array[String]))] = allHotspotUpdates
        .map(update => { // n*INCIDENT_WEIGHT and increment hotspot age
        val detailsArr: Array[String] = update._2._2
          detailsArr(INDEX_HS_AGE) = (detailsArr(INDEX_HS_AGE).toInt + 1).toString
          (update._1, (update._2._1.toDouble*INCIDENT_WEIGHT, detailsArr))
        })
        .union(initialHotspotsRDD
          .map(hs => { // n*INCIDENT_WEIGHT and increment hotspot age
            val detailsArr: Array[String] = hs._2._2
            detailsArr(INDEX_HS_AGE) = (/*detailsArr(INDEX_HS_AGE).toInt +*/ 1).toString
            (hs._1, (hs._2._1.toDouble*INCIDENT_WEIGHT, detailsArr))
        }))

      // includes decay of fitness of all existing hotspots => corresponds to oldFitness*(1-DECAY_RATE) part of fitness formula
      val unionEverything = weightedUpdates
        .union(hotspotCache
          .map(hotspot => { // decay fitness, and increment hotspot age
          val detailsArr: Array[String] = hotspot._2._2
            detailsArr(INDEX_HS_AGE) = (detailsArr(INDEX_HS_AGE).toInt + 1).toString
            (hotspot._1, (hotspot._2._1*(1.0-decayRate),detailsArr))
          }))
        .coalesce(numPartitions) // coalesce added to stop accumulation of partitions in state RDD over time

      val timeBeforeFinalRed = System.nanoTime()

      // aggregate fitness for hotspots
      val newStateBeforeDelete: RDD[((Double, Double), (Double, Array[String]))] = unionEverything
        .reduceByKey((a, b) => ((a._1 + b._1), a._2))

      val beforeDelete = newStateBeforeDelete.count()

      // remove hotspots with fitness < deletion threshold
      val newState = newStateBeforeDelete.filter(hotspot => (hotspot._2._1 >= deleteThreshold))

      val newStateCount = newState.count()

      val deleted = beforeDelete - newStateCount
      println("\n we have removed this many hotspots (< DELETE_THRESHOLD): "+deleted)

      println("\nnew state: " + newStateCount)
      val timeForFinalRed = (System.nanoTime - timeBeforeFinalRed)/ 1e9
      println("\nTime for final reduce: "+timeForFinalRed)

      val timeBeforeCache = System.nanoTime()

      hotspotCache.unpersist() 
      hotspotCache = newState 
      hotspotCache.cache // cache new state, so can access in next streaming batch

      println("\nhotspot cache count: "+hotspotCache.count())
      val timeForCache = (System.nanoTime - timeBeforeCache)/ 1e9
      println("\nTime for cache: "+timeForCache)

      // filter hotspots => only want to output those that have a fitness > HOTSPOT_THRESHOLD
      val actualHotspots = newState.filter(hotspot => hotspot._2._1 >= hotspotThreshold)
      val actualHotspotsCount = actualHotspots.count()
      println("\n Actual Hotspots: " + actualHotspotsCount)

      val stage3Time = (System.nanoTime - beforeStage3)/ 1e9
      println("\nSTAGE 3 TIME: "+stage3Time)

      val runtime = (System.nanoTime - startTime)/ 1e9
      println("\nRUNTIME: "+runtime)


      // print results
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
      streamOutput += "\n************   END   **************"

      println(streamOutput)

      var directory = new File("./"+ outputDir + "/algorithmDetails/")

      if(!directory.exists()){
        println("\nDIR DOES NOT EXIST")
        directory.mkdirs()
      }

      var pw = new PrintWriter(new File("./"+ outputDir + "/algorithmDetails/interval_" + System.nanoTime() ))
      pw.write(streamOutput)
      pw.close()

      val testcount1 = actualHotspots.mapPartitionsWithIndex((index, xs) => {
        print("PART ACT "+index+": ")
        while(xs.hasNext) {
          val x: ((Double, Double), (Double, Array[String])) = xs.next()
          print(","+x._2._2(INDEX_HS_AGE))
        }
        print("\n")
        xs
      }).count()

      val testcount2 = newState.mapPartitionsWithIndex((index, xs) => {
        print("PART STATE "+index+": ")
        while(xs.hasNext) {
          val x: ((Double, Double), (Double, Array[String])) = xs.next()
          print(","+x._2._2(INDEX_HS_AGE))
        }
        print("\n")
        xs
      }).count()

      // return actual hotspots 
      actualHotspots
    })

    // format hot spots as strings: latitude longitude fitness age
    val hotspotStrings: DStream[String] = actualHotspots.map(hotspot => hotspot._1._1 + "\t" + hotspot._1._2 + "\t" + hotspot._2._1 + "\t" + hotspot._2._2(INDEX_HS_AGE))

    // output options for the hot spots
    hotspotStrings.saveAsTextFiles(outputDir + "/hotspotDetails/interval") // saves output in dir prefix-TIME
    //    hotspotStrings.foreachRDD(rdd => { // overwrites previous output each batch
    //
    //      println("\nPartitions at end: "+rdd.partitions.size)
    //      val timeBeforeWrite = System.nanoTime()
    //
    //      val outputDir_time: String = outputDir + "/hotspotDetails"
    //      val redPartitions: RDD[String] = rdd.coalesce(numPartitions) // reduce number of partitions so we only have to write 4 files
    //      redPartitions.saveAsTextFile(outputDir_time) // one file written per partition
    //
    //      println("Reduced Partitions: "+redPartitions.partitions.size)
    //      val timeForWrite = (System.nanoTime - timeBeforeWrite)/ 1e9
    //      println("\nTime for coalesce: "+timeForWrite)
    //    })
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

      //ssc.awaitTermination()
       ssc.stop()
/*      if (listen.getNBatches().toInt==numBatches ) {
        println ("STOPPING execution")

      }
*/

     // ssc.awaitTermination() // wait for streaming computation to finish before exiting application

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
  def getMiles(incident: ((Double, Double), Array[String]), hotspot: ((Double, Double), Array[String])): Double = {

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

  /** Removes redundancies within a set of incidents - Stage 2
    * Collects RDD back to driver so must be small set of incidents
    *
    * @param incidents Array of incident information ((lat,lng), n)
    *                  where n represents number of incidents represented by this location
    */
  def reduceIncidentsNew(incidents: Array[((Double, Double), (Int, Array[String]))], mileage: Double): Array[((Double, Double), (Int, Array[String]))] = {

    // incidents should be relatively small as we have already removed any incidents that can be allocated to existing hotspots
    val incidentsArr: Array[((Double, Double), (Int, Array[String]))] = incidents
    val centres = mutable.HashMap.empty[(Double,Double), (Int,Array[String])] // this will store the resulting hotspot centres and their fitnesses

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

          centres += incidentsArr(i) // add incident as centre in its own right

        } else if(centres.contains(incidentsArr(closestIncident.get._1)._1)) { // closest incident already in centres

          val closestIndex = closestIncident.get._1
          val centreLocation: (Double, Double) = incidentsArr(closestIndex)._1 // key in HashMap
          val newFitness: Int = centres(centreLocation)._1 + incidentsArr(i)._2._1
          val replaceCentre = (centreLocation,(newFitness, centres(centreLocation)._2))
          centres += replaceCentre // replace the previously existing instance of key in HashMap

        } else { // closest incident not yet in centres

          val closestIndex = closestIncident.get._1
          val centreLocation: (Double, Double) = incidentsArr(closestIndex)._1
          val newFitness: Int = incidentsArr(closestIndex)._2._1 + incidentsArr(i)._2._1
          val newCentre = (centreLocation,(newFitness, incidentsArr(closestIndex)._2._2))
          centres += newCentre // add centre to HashMap
        }

      }

    }

    // at this point, centres stores all the hotspot centres within the new incidents, as well as their fitnesses
    // convert to array so that we can parallelize() it, then return
    val centresArr: Array[((Double, Double), (Int, Array[String]))] = centres.toArray
    centresArr

  }

  /** Allocates new incidents to current hotspots, per partition - Stage 1
    * Incident (lat,lng) is replaced with hotspot (lat,lng) if suitable hotspot is found
    *
    * @param iter iterator of new incident locations
    * @param currentHotspotsBroadcast Broadcast variable, array of hotspot locations
    * @param index Index of the partition
    */
  def reduceWithHotspots(iter: Iterator[((Double, Double), Array[String])],
                         currentHotspotsBroadcast: Broadcast[Array[((Double,Double), (Double, Array[String]))]],
                         index: Int,
                         mileage: Double): Iterator[((Double,Double),(Int, Array[String], Boolean))] = {

    val currentHotspots: Array[((Double, Double), Array[String])] = currentHotspotsBroadcast.value.map(h => (h._1, h._2._2))
    var allocatedIncidents = new ListBuffer[((Double, Double), (Int, Array[String], Boolean))] // will store incidents keyed by their updated (lat,lng)

    while(iter.hasNext) {

      val incident: ((Double, Double), Array[String]) = iter.next()
      var closestHotspot: Option[((Double,Double),(Double, Array[String]))] = None // Option[((lat,lng),(miles, hotspot info))]

      breakable {
        for(i <- 0 until currentHotspots.size) { // compare against all current hotspots

          val miles = getMiles(incident, currentHotspots(i)) // calculate Haversine distance between incident and hot spot

          if(miles != -1.0) { // miles == -1.0 would mean hot spot and incident were not on the same road

            if(miles <= mileage) { // incident falls within radius of hot spot

              if(closestHotspot.isEmpty) { // not yet found hot spot close enough
                closestHotspot = Some((currentHotspots(i)._1,(miles, currentHotspots(i)._2)))
              } else if(miles < closestHotspot.get._2._1) { // hotspot i is closer than previously found hotspot
                closestHotspot = Some((currentHotspots(i)._1,(miles, currentHotspots(i)._2)))
              }
            }
          }

          // if found a close enough hotspot, we can break out of the loop
          if(!closestHotspot.isEmpty) {
            break
          }
        }
      }

      // if closestHotspot is still None, then no hot spot was found close enough => keep incident's original (lat,lng)
      // otherwise => update incident's (lat,lng) to be the same as the hotspot's
      // 1: each key-value pair represents 1 incident => these will be aggregated in future reduceByKey to track how many incidents should be allocated to each hotspot
      // boolean parameter: true if incident not allocated to hotspot yet => marks it as part of the set to be reduced in Stage 2
      val updatedIncident = if (closestHotspot.isEmpty) (incident._1, (1, incident._2, true)) else (closestHotspot.get._1, (1, closestHotspot.get._2._2, false))
      allocatedIncidents += updatedIncident

    }

    // return iterator of all allocated incidents
    val result = allocatedIncidents.toIterator
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

