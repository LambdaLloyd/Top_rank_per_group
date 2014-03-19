package nl.lambdalloyd

import org.scalatest._
import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.meta._
import scala.slick.jdbc.StaticQuery._
import scala.slick.jdbc.StaticQuery

class TopNrankPureSLICKSuite extends FunSuite with BeforeAndAfter {
  val topN = 3
  // The query interface for the Emp and H2 provided dual table
  val employees = TableQuery[Emp]
  val dual = TableQuery[Dual]

  implicit var session: Session = _
  var allQueryCompiled: scala.slick.lifted.CompiledFunction[slick.driver.H2Driver.simple.Column[Int] => scala.slick.lifted.Query[( //
  scala.slick.lifted.Column[Int], //
  scala.slick.lifted.Column[String], //
  scala.slick.lifted.Column[String], //
  scala.slick.lifted.Column[Option[String]], //
  scala.slick.lifted.Column[Option[Double]], //
  scala.slick.lifted.Column[Int], //
  scala.slick.lifted.Column[Int]), (Int, String, String, Option[String], Option[Double], Int, Int)], slick.driver.H2Driver.simple.Column[Int], Int, //
  scala.slick.lifted.Query[(scala.slick.lifted.Column[Int], //
  scala.slick.lifted.Column[String], //
  scala.slick.lifted.Column[String], //
  scala.slick.lifted.Column[Option[String]], //
  scala.slick.lifted.Column[Option[Double]], //
  scala.slick.lifted.Column[Int], //
  scala.slick.lifted.Column[Int]), //
  (Int, String, String, Option[String], Option[Double], Int, Int)], Seq[(Int, String, String, Option[String], Option[Double], Int, Int)]] = _

  //  val db = Database.forURL(
  //    "jdbc:h2:mem:test1;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0;FILE_LOCK=SOCKET",
  //    driver = "org.h2.Driver")

  val db = Database.forURL(
    "jdbc:h2:~/test;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0",
    user = "sa",
    password = "",
    prop = null,
    driver = "org.h2.Driver")

  // Enumerates the sections
  import TopNrankPureSLICK.Sections._

  def headLineQuery(topN: Column[Int]) =
    dual.map { case (_) => (HeadLine.id, "", "", Option("0"), Option(0.0), 0, topN) }

  /** The second part of the union all query
   *  Summarize the total table
   */
  def totSummaryQuery = dual.map(c => (TotSummary.id, "", "", Option("0"),
    employees.map(_.salary).avg,
    employees.map(_.deptId).countDistinct,
    employees.map(_.id).length))

  /** The third part of the union all query
   *  Separator line between departments
   *  Note: dept variable is only for sorting purpose
   */
  def depSepaQuery = employees.groupBy(_.deptId)
    .map { case (dept, _) => (DeptSeparator.id, "", "", dept, Option(0.0), 0, 0) }

  /** The fourth part of the union all query
   *  Summarize the department
   */
  def depSummaryQuery = employees.groupBy(_.deptId)
    .map { case (dept, css) => (DeptSummary.id, "", "", dept, css.map(_.salary).avg, 0, css.length) }

  /** The sixth part of the union all query
   *  The column names are printed.
   */
  def depColNamesQuery = employees.groupBy(_.deptId)
    .map { case (dept, _) => (DeptColNames.id, "", "", dept, Option(0.0), 0, 0) }

  /** The seventh part of the union all query
   *  A ruler to format the table
   */
  def depColLineal = employees.groupBy(_.deptId)
    .map { case (dept, _) => (DeptColLineal.id, "", "", dept, Option(0.0), 0, 0) }

  /** The main part of the union all query - here is the beef
   *  To display the top employees by:
   *  id, name, salary. Dept is not displayed, only for sorting purpose.
   */
  def mainQuery(topN: Column[Int]) = {
    def countColleaguesHasMoreOrSame(empId: Column[String]) = (
      for {
        (e3, e4) <- employees innerJoin employees
        if (empId === e3.id) && (e3.deptId === e4.deptId) && (e3.salary <= e4.salary)
      } yield (e4.salary)).countDistinct

    /** Returns the number of equal salaries in a department. Normally 1.
     *  For checking ties, a.k.a ex aequo.
     */
    def countSalaryTies(deptId: Column[Option[String]], salary: Column[Option[Double]]) =
      employees.filter(e1 => (deptId === e1.deptId) && (salary === e1.salary)).length

    employees.map {
      case (row) => (MainSection.id,
        row.id,
        row.name,
        row.deptId,
        row.salary,
        countColleaguesHasMoreOrSame(row.id), /*a.k.a. rank*/
        countSalaryTies(row.deptId, row.salary))
    }.filter(_._6 /*In fact countColleaguesHasMoreOrSame*/ <= topN)
  } // mainQuery

  //** The last part of the union all query*/
  def bottomLineQuery = TableQuery[Dual].map(c =>
    (BottomLine.id, "", "", Option(None + "") /*Work around, evaluates to Some(None) ment is None*/ ,
      Option(0.0), 0, mainQuery(topN).length))

  /** Compose the query with above queries and union all's*/
  def allQuery(topN0: Column[Int]) =
    (headLineQuery(topN0) // Run-time error TopNrankPureSLICK.headLineQuery(topN0)
      ++ totSummaryQuery
      ++ depSepaQuery
      ++ depSummaryQuery
      ++ depColNamesQuery
      ++ depColLineal
      ++ mainQuery(topN0)
      ++ bottomLineQuery) // Sort based on the following columns
      .sortBy(_._5.desc.nullsLast) // salary
      .sortBy(_._1) //section
      .sortBy(_._4.nullsLast) //deptId

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

  // Create a connection (called a "session") to an in-memory H2 database
  // NO autocommit !
  db.withTransaction {
    implicit session =>
      val tables = MTable.getTables().list()
      if (tables.count(_.name.name.equals(Emp.TABLENAME)) == 1) println("Emp exists, will be initialized.")
      //sql"drop table if exists ${Emp.TABLENAME}".as[String].execute // Doesn't work
      (scala.slick.jdbc.StaticQuery.u + s"drop table if exists ${Emp.TABLENAME}").execute

      // Create the schema
      employees.ddl.create

      // Fill the database, commit work if success
      session.withTransaction {
        employees ++= Emp.testContent
      }

      allQueryCompiled = Compiled(allQuery(_)) // This forces the parameter must be Column[Int]      
  }

  before {
    session = db.createSession
  }

  test("Table is total filled") {

    assert(employees.length.run === Emp.testContent.size)
  }

  test("Test the first row") {
    val ist = allQueryCompiled(topN).run
    assert(ist.head === soll.head)
  }

  test("Test all rows") {
    val ist = allQueryCompiled(topN).run
    assert(ist === soll)
  }

  test("Conversion to output") {
    allQueryCompiled(topN).foreach {

      row => println(TopNrankPureSLICK.presentation(row))
    }
  }

  after {
    session.close()
  }
}