package nl.lambdalloyd
package slickeval

import scala.slick.driver.{ ExtendedProfile, /*JdbcProfile,*/ H2Driver /*, SQLiteDriver */ }
import com.typesafe.slick.driver.oracle.OracleDriver

class RichOracle extends OracleDriver { def toString = "OracleDriver" }
class RichH2Driver extends OracleDriver { def toString = "H2Driver" }
class RichXX extends OracleDriver { def toString = "OracleDriver" }
class RichX extends OracleDriver { def toString = "OracleDriver" }

protected object PortableDriver {
  val simple = profile.simple

  final val defaultSchema = "HR" // ???? Must be a def, lazy val or a final val for Oracle login ????

  lazy val (profile: ExtendedProfile,
    db: scala.slick.jdbc.JdbcBackend#DatabaseDef,
    schema: Option[String],
    detector: String) =
    scala.util.Properties.propOrElse("database", "H2mem") match {
      case st if st == "oracle" => (
        OracleDriver,
        OracleDriver.simple.Database.forURL("jdbc:oracle:thin:@localhost:1521/LAB0",
          Map("user" -> defaultSchema, "password" -> "hr")),
          {
            Class.forName("oracle.jdbc.OracleDriver") // Is needed, otherwise "No suitable driver found for" exception      
            Option(defaultSchema)
          },
          "oracle")
      case st if st == "H2svr" => (
        H2Driver,
        H2Driver.simple.Database.forURL(
          """jdbc:h2:tcp://localhost/~/testSlickSvr;USER=HR;PASSWORD=hr;AUTOCOMMIT=OFF;WRITE_DELAY=300;LOCK_MODE=1""",
          driver = "org.h2.Driver"),
          None,
          "H2svr")
      case _ => (
        H2Driver,
        H2Driver.simple.Database.forURL("jdbc:h2:mem:;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None,
          "H2mem")

      /*      case _ => ({
        System.clearProperty("database")
        H2Driver
      },
        H2Driver.simple.Database.forURL("jdbc:h2:~/testSlickMem;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None,
          "?")
*/ }

  def driverName: String =
    profile.simple.getClass().toString().takeWhile(_ != '$').reverse.takeWhile(_ != '.').reverse

  def doConditionalPrintSQL(statement: String) { // Inspect generated SQL  
    if (scala.util.Properties.propOrFalse("printSQL")) {
      println(statement)
      println
    }
  }

  if (scala.util.Properties.propOrFalse("verbose")) {
    Emp.printJvmBanner()
    println(s"Running with database driverclass: ${PortableDriver.driverName} configurated by ${PortableDriver.detector} \n")
  }

}

