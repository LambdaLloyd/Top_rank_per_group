package nl.lambdalloyd

import scala.slick.driver.{ ExtendedProfile, /*JdbcProfile,*/ H2Driver /*, SQLiteDriver */ }
import com.typesafe.slick.driver.oracle.OracleDriver

trait PortableDriverTrait {
  val simple = profile.simple

  def stringToBoolean(s: String): Boolean =
    (s != null) && (s.toUpperCase match {
      case "TRUE" | "T" | "YES" | "Y" => true
      case _                          => false
    })

  //System.setProperty("database", "oracle")

  import nl.lambdalloyd.{ Databases => D }
  lazy val defaultSchema = "HR" // ???? Must be a def, lazy val or a final val for Oracle login ????

  //  def a = D.Ora12
  //  def rdbms = (0 match { case 0 => a })

  /*  lazy val (profile: ExtendedProfile,
    db: scala.slick.jdbc.JdbcBackend#DatabaseDef,
    schema: Option[String]) =
    System.getProperty("database") match {
      case "oracle" => (OracleDriver,
        OracleDriver.simple.Database.forURL("jdbc:oracle:thin:@localhost:1521/LAB0",
          Map("user" -> defaultSchema, "password" -> "hr")),
          {
            Class.forName("oracle.jdbc.OracleDriver") // Is needed, otherwise "No suitable driver found for" exception      
            Option(defaultSchema)
          })
      case "sqlite" => (SQLiteDriver,
        SQLiteDriver.simple.Database.forURL("jdbc:h2:~/testSlick;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None)
      case _ => (H2Driver,
        H2Driver.simple.Database.forURL(
          "jdbc:h2:tcp://localhost/~/testSlick;AUTOCOMMIT=OFF;WRITE_DELAY=300;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None)
    }*/

  lazy val (profile: ExtendedProfile,
    db: scala.slick.jdbc.JdbcBackend#DatabaseDef,
    schema: Option[String]) =

    PortableDriver.rdbms match {
      case D.Ora12 => (OracleDriver,
        OracleDriver.simple.Database.forURL("jdbc:oracle:thin:@localhost:1521/LAB0",
          Map("user" -> defaultSchema, "password" -> "hr")),
          {
            Class.forName("oracle.jdbc.OracleDriver") // Is needed, otherwise "No suitable driver found for" exception      
            Option(defaultSchema)
          })
      //      case "sqlite" => (SQLiteDriver,
      //        SQLiteDriver.simple.Database.forURL("jdbc:h2:~/testSlick;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
      //          driver = "org.h2.Driver"),
      //          None)
      case D.H2svr => (H2Driver,
        H2Driver.simple.Database.forURL(
          "jdbc:h2:tcp://localhost/~/testSlick;AUTOCOMMIT=OFF;WRITE_DELAY=300;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"),
          None)
    }

  lazy val driverName: String =
    profile.simple.getClass().toString().takeWhile(_ != '$').reverse.takeWhile(_ != '.').reverse
}
