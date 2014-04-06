/**
 */
package nl.lambdalloyd

/** @author FransAdm
 *
 */
import org.scalatest._

import PortableDriver.simple._

class TableH2embSuite extends TablesTest {
//    System.clearProperty("database")
  System.setProperty("database", "H2emb")
}

/*object TableH2embSuite {
  System.clearProperty("database")
  System.setProperty("database", "H2emb")
}*/