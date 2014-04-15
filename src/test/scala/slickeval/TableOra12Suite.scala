/**
 */
package nl.lambdalloyd
package slickeval

/** @author FransAdm
 *
 */
import org.scalatest._
import PortableDriver.simple._

class TableOra12Suite extends TablesTest {
  //*object TableOra12Suite {
  System.clearProperty("database")
  System.setProperty("database", "oracle")
}