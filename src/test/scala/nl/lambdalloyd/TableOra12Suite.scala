/**
 */
package nl.lambdalloyd

/** @author FransAdm
 *
 */
import org.scalatest._

import PortableDriver.simple._

class TableOra12Suite extends TablesTest {
//  System.clearProperty("database")
  System.setProperty("database", "oracle")
}

/*object TableOra12Suite {
  System.clearProperty("database")
  System.setProperty("database", "oracle")
}*/