package nl.lambdalloyd

import scala.slick.driver.H2Driver.simple._
//import scala.slick.driver.H2Driver.simple.Database
//import scala.slick.driver.H2Driver.simple.Session
//import scala.slick.lifted.{ Column, TableQuery }

import org.scalatest._
import scala.slick.jdbc.meta._
import scala.slick.lifted.Compiled
import scala.slick.jdbc.StaticQuery.u

class TopNrankPureSLICKSuite extends FunSuite with BeforeAndAfter {
  import TopNrankPureSLICKSuite._
  val allQueryCompiled = Compiled(allQuery(_)) // This forces the parameter must be Column[Int]      

  before {
    session = db.createSession
  }

  test("Table is total filled") { assert(employees.length.run === Emp.testContent.size) }

  test("Test the first row") {
    val ist = allQueryCompiled(topN).run
    assert(ist.head === soll.head)
  }

  test("Query all rows works") {
    val ist = allQueryCompiled(topN).run
    assert(ist === soll)
  }

  test("Conversion to output works") {
    val ist = allQueryCompiled(topN).run.map(row => TopNrankPureSLICK.presentation(row))
    assert(expected === ist)
  }

  after {
    session.close()
  }
}

object TopNrankPureSLICKSuite extends TopNrankPureSLICKtrait {

  implicit var session: Session = _

  //  val db = Database.forURL(
  //    "jdbc:h2:mem:test1;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
  //    driver = "org.h2.Driver")

  val db = Database.forURL(
    "jdbc:h2:~/test;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0",
    user = "sa",
    password = "",
    prop = null,
    driver = "org.h2.Driver")

  import Sections._
  val soll: Vector[(Int, String, String, Option[String], Option[Double], Int, Int)] =
    Vector((HeadLine.id, "", "", Option("0"), Option(0.0), 0, 3),
      (TotSummary.id, "", "", Option("0"), Option(500050.0 / 15.0), 4, 16),
      (DeptSeparator.id, "", "", Option("D050"), Option(0.0), 0, 0),
      (DeptSummary.id, "", "", Option("D050"), Option(34450.0), 0, 3),
      (DeptColNames.id, "", "", Option("D050"), Option(0.0), 0, 0),
      (DeptColLineal.id, "", "", Option("D050"), Option(0.0), 0, 0),
      (MainSection.id, "E21437", "John Rappl", Option("D050"), Option(47000.0), 1, 1),
      (MainSection.id, "E41298", "Nathan Adams", Option("D050"), Option(21900.0), 2, 1),
      (MainSection.id, "E21438", "trainee", Option("D050"), None, 0, 0),
      (DeptSeparator.id, "", "", Option("D101"), Option(0.0), 0, 0),
      (DeptSummary.id, "", "", Option("D101"), Option(33980.0), 0, 5),
      (DeptColNames.id, "", "", Option("D101"), Option(0.0), 0, 0),
      (DeptColLineal.id, "", "", Option("D101"), Option(0.0), 0, 0),
      (MainSection.id, "E00127", "George Woltman", Option("D101"), Option(53500.0), 1, 1),
      (MainSection.id, "E04242", "David McClellan", Option("D101"), Option(41500.0), 2, 1),
      (MainSection.id, "E10297", "Tyler Bennett", Option("D101"), Option(32000.0), 3, 1),
      (DeptSeparator.id, "", "", Option("D190"), Option(0.0), 0, 0),
      (DeptSummary.id, "", "", Option("D190"), Option(36675.0), 0, 4),
      (DeptColNames.id, "", "", Option("D190"), Option(0.0), 0, 0),
      (DeptColLineal.id, "", "", Option("D190"), Option(0.0), 0, 0),
      (MainSection.id, "E10001", "Kim Arlich", Option("D190"), Option(57000.0), 1, 1),
      (MainSection.id, "E16398", "Timothy Grove", Option("D190"), Option(29900.0), 2, 3),
      (MainSection.id, "E16399", "Timothy Grave", Option("D190"), Option(29900.0), 2, 3),
      (MainSection.id, "E16400", "Timothy Grive", Option("D190"), Option(29900.0), 2, 3),
      (DeptSeparator.id, "", "", Option("D202"), Option(0.0), 0, 0),
      (DeptSummary.id, "", "", Option("D202"), Option(28637.5), 0, 4),
      (DeptColNames.id, "", "", Option("D202"), Option(0.0), 0, 0),
      (DeptColLineal.id, "", "", Option("D202"), Option(0.0), 0, 0),
      (MainSection.id, "E01234", "Rich Holcomb", Option("D202"), Option(49500.0), 1, 1),
      (MainSection.id, "E39876", "Claire Buckman", Option("D202"), Option(27800.0), 2, 1),
      (MainSection.id, "E27002", "David Motsinger", Option("D202"), Option(19250.0), 3, 1),
      (BottomLine.id, "", "", Option(None + ""), // Work around, evaluates to Some(None), mend is None
        Option(0.0), 0, 13))

  val expected = Vector(
    "Report top  3 ranks for each department.",
    "Tot. 16 employees in 4 deps.Avg. sal.: 33336,67",
    "-",
    "Department: D050, pop:  3.Avg Salary:  34450,00",
    "   Employee ID       Employee name   SalaryRank",
    "-------------+-------------------+--------+---+",
    "        E21437          John Rappl 47000,00   1",
    "        E41298        Nathan Adams 21900,00   2",
    "        E21438             trainee     null   -",
    "-",
    "Department: D101, pop:  5.Avg Salary:  33980,00",
    "   Employee ID       Employee name   SalaryRank",
    "-------------+-------------------+--------+---+",
    "        E00127      George Woltman 53500,00   1",
    "        E04242     David McClellan 41500,00   2",
    "        E10297       Tyler Bennett 32000,00   3",
    "-",
    "Department: D190, pop:  4.Avg Salary:  36675,00",
    "   Employee ID       Employee name   SalaryRank",
    "-------------+-------------------+--------+---+",
    "        E10001          Kim Arlich 57000,00   1",
    "        E16398       Timothy Grove 29900,00  T2",
    "        E16399       Timothy Grave 29900,00  T2",
    "        E16400       Timothy Grive 29900,00  T2",
    "-",
    "Department: D202, pop:  4.Avg Salary:  28637,50",
    "   Employee ID       Employee name   SalaryRank",
    "-------------+-------------------+--------+---+",
    "        E01234        Rich Holcomb 49500,00   1",
    "        E39876      Claire Buckman 27800,00   2",
    "        E27002     David Motsinger 19250,00   3",
    "  13 Employees are listed.")

  // Create a connection (called a "session") to an in-memory H2 database
  // NO autocommit !
  db.withTransaction {
    implicit session =>
      val tables = MTable.getTables().list()

      if (tables.map(_.name.name).contains(Emp.TABLENAME)) println("Emp exists, will be rebuild.")

      //sql"drop table if exists ${Emp.TABLENAME}".as[String].execute // Doesn't work
      (u + s"drop table if exists ${Emp.TABLENAME}").execute

      // Create the schema
      employees.ddl.create

      // Fill the database, commit work if success
      session.withTransaction {
        employees ++= Emp.testContent
      }

    //       allQueryCompiled = Compiled(allQuery(_)) // This forces the parameter must be Column[Int]      

  }
} // object TopNrankPureSLICKSuite