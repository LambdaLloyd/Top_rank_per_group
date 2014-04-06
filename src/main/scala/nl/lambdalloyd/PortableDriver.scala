package nl.lambdalloyd

import scala.slick.driver.{ ExtendedProfile, /*JdbcProfile,*/ H2Driver /*, SQLiteDriver */ }
import com.typesafe.slick.driver.oracle.OracleDriver

object PortableDriver {
  lazy val simple = profile.simple

  def stringToBoolean(s: String): Boolean =
    (s != null) && (s.toUpperCase match {
      case "TRUE" | "T" | "YES" | "Y" => true
      case _                          => false
    })

  final def databaseSelector() = { System.getProperty("database") }

  final val defaultSchema = "HR" // ???? Must be a def, lazy val or a final val for Oracle login ????

  lazy val (profile: ExtendedProfile,
    db: scala.slick.jdbc.JdbcBackend#DatabaseDef,
    schema: Option[String],
    detector: String) =

    databaseSelector match {
      case st if st == "oracle" => ({
        //System.clearProperty("database")
        OracleDriver
      },
        OracleDriver.simple.Database.forURL("jdbc:oracle:thin:@localhost:1521/LAB0",
          Map("user" -> defaultSchema, "password" -> "hr")),
          {
            Class.forName("oracle.jdbc.OracleDriver") // Is needed, otherwise "No suitable driver found for" exception      
            Option(defaultSchema)
          },
          "oracle")
      case st if st == "H2emb" => ({
        //System.clearProperty("database")
        H2Driver
      },
        H2Driver.simple.Database.forURL("jdbc:h2:~/testSlickMem;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None,
          "H2emb")
      case st if st == "H2svr" => ({
        //System.clearProperty("database")
        H2Driver
      },
        H2Driver.simple.Database.forURL(
          "jdbc:h2:tcp://localhost/~/testSlickSvr;AUTOCOMMIT=OFF;WRITE_DELAY=300;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None,
          "H2svr")
/*      case _ => ({
        System.clearProperty("database")
        H2Driver
      },
        H2Driver.simple.Database.forURL("jdbc:h2:~/testSlickMem;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None,
          "?")
*/    }

  lazy val driverName: String =
    profile.simple.getClass().toString().takeWhile(_ != '$').reverse.takeWhile(_ != '.').reverse
}

