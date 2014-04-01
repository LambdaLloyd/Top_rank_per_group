package nl.lambdalloyd

import scala.slick.driver.{ ExtendedProfile, JdbcProfile, H2Driver, SQLiteDriver }
import com.typesafe.slick.driver.oracle.OracleDriver

object PortableDriver {
  val simple = profile.simple

  def stringToBoolean(s: String) = s match {
    case "TRUE" | "T" | "YES" | "Y" => true
    case _                          => false
  }

  //System.setProperty("database", "oracle")

  lazy val (profile: ExtendedProfile, db: scala.slick.jdbc.JdbcBackend#DatabaseDef) =
    System.getProperty("database") match {
      case "oracle" => (OracleDriver,
        OracleDriver.simple.Database.forURL("jdbc:oracle:thin:@//localhost:1521/LAB0", Map("user" -> "hr", "password" -> "hr")))
      case "sqlite" => (SQLiteDriver,
        SQLiteDriver.simple.Database.forURL("jdbc:h2:mem:test1;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"))
      case _ => (H2Driver,
        H2Driver.simple.Database.forURL(
          "jdbc:h2:mem:test1;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
          driver = "org.h2.Driver"))
    }

  if (stringToBoolean(System.getProperty("verbose"))) {
    val driverName =
      profile.simple.getClass().toString().takeWhile(_ != '$').reverse.takeWhile(_ != '.').reverse
    println(s"Driver is: $driverName")
  }
}
