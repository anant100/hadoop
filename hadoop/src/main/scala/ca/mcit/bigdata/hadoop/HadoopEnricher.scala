package ca.mcit.bigdata.hadoop

import ca.mcit.bigdata.hadoop.schema.{Calendar, Route, Trip}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}

object HadoopEnricher extends Main with App {

  val fileList = fs.listStatus(new Path("/user/summer2019"))

  /** Start data input streams */
  val routeStream: FSDataInputStream = fs.open(new Path(("/user/summer2019/thakkar/stm/routes.txt")))
  val tripStream: FSDataInputStream = fs.open(new Path("/user/summer2019/thakkar/stm/trips.txt"))
  val calendarStream: FSDataInputStream = fs.open(new Path("/user/summer2019/thakkar/stm/calendar.txt"))

  /** Using Iterator Stream by Mapping data in memory ---------------Method 1--------------------- */
  /** Route iterator */
  val routeInput: Iterator[String] = Iterator.continually(routeStream.readLine()).takeWhile(_ != null)
  var lookupMap: Map[String, Any] = Map() // Initialize empty Map()
  val routeHeader: String = routeInput.next() // Get the Header of Route
  while (routeInput.hasNext) {
    val nextRoute = Route(routeInput.next())
    lookupMap ++= Map(nextRoute.route_id.toString -> nextRoute) // Map Route data into memory
  }
  routeStream.close() // Close Route Input Stream

  /** Calendar iterator */
  val calendarInput: Iterator[String] = Iterator.continually(calendarStream.readLine()).takeWhile(_ != null)
  val calendarHeader: String = calendarInput.next() // Get the Header for Calendar
  while (calendarInput.hasNext) {
    val nextCalendar = Calendar(calendarInput.next())
    lookupMap ++= Map(nextCalendar.service_id -> nextCalendar) // Map Calendar Data into memory
  }
  calendarStream.close() // Close Calendar Input Stream

  /** Create OR Replace(If already Exists) Directory to HDFS */
  val outputDir = new Path("/user/summer2019/thakkar/course3")
  if (fs.exists(outputDir)) fs.delete(outputDir, true)
  fs.mkdirs(outputDir)

  val outputPath = new Path("/user/summer2019/thakkar/course3/EnrichedOutput.csv") // Output File path
  val outputFile: FSDataOutputStream = fs.create(outputPath, true) // Start Output stream in Output file

  /** Trip Iterator */
  val tripInput: Iterator[String] = Iterator.continually(tripStream.readLine()).takeWhile(_ != null)
  val tripHeader: String = tripInput.next() // Get the Header of Trip

  outputFile.writeBytes(tripHeader + "," + routeHeader + "," + calendarHeader) // Write Header of CSV file (Trip + Route + Calendar)

  while (tripInput.hasNext) {
    val trip = Trip(tripInput.next()) // get Trip String one by one

    outputFile.writeBytes("\n")
    outputFile.writeBytes(Trip.toCsv(trip) + "," + Route.toCsv(lookupMap(trip.route_id.toString).asInstanceOf[Route]) + "," + Calendar.toCsv(lookupMap(trip.service_id).asInstanceOf[Calendar]))
  }

  tripStream.close()
  outputFile.close()
  fs.close()
}