/**
 */
package nl.lambdalloyd

/** @author FransAdm
 *
 */
import org.scalatest._

import PortableDriver.simple._

class TableH2svrSuite extends TablesTest {
//  System.clearProperty("database")
  System.setProperty("database", "H2svr")
}

/*object TableH2svrSuite {
  System.clearProperty("database")
  System.setProperty("database", "H2svr")
}*/