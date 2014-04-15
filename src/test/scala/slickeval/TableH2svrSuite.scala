/**
 */
package nl.lambdalloyd
package slickeval

/** @author FransAdm
 *
 */
import org.scalatest._

class TableH2svrSuite extends TablesTest {
  //  System.clearProperty("database")
  System.setProperty("database", "H2svr")
}