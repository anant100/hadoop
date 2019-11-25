package ca.mcit.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Main {

  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))
  //Set dynamic value by using this --> conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")

  val fs = FileSystem.get(conf)

}
/** Copy files from local system to HDFS */
/*
fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/trips.txt"), new Path("/user/summer2019/thakkar/trips.txt"))
fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/routes.txt"), new Path("/user/summer2019/thakkar/routes.txt"))
fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/routes1.txt"), new Path("/user/summer2019/thakkar/routes1.txt"))
fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/calendar.txt"), new Path("/user/summer2019/thakkar/calendar.txt"))*/

