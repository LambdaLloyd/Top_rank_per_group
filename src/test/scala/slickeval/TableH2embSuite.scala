/**
 */
package nl.lambdalloyd
package slickeval

/** @author FransAdm
 *
 */
import org.scalatest._

class TableH2embSuite extends TablesTest {
  System.setProperty("database", "H2mem")
}