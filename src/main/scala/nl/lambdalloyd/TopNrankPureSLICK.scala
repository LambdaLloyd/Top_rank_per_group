package nl.lambdalloyd
import scala.slick.driver.H2Driver.simple._
import scala.slick.lifted.ProvenShape

/** In pure SLICK, find the top N salaries in each department, where N is provided as a parameter.
 *
 *  @version			0.9	2014-03-18
 *  @author		Frans W. van den Berg
 *
 *  How it works:
 *  One composed query produces all the rows necessary for the output. The composed query is
 *  made of a union (union all) of queries mostly making a row per department. Two queries
 *  making aggregated data of the whole table to summarize the payroll. All rows are tagged
 *  by a section id in the first column.
 *  The core query of it produces a row per employee with the personal data by counting per
 *  employee the employees (and its self) who are earning the same or more in a department.
 *  This number of employees is the ranking number which is filtered to be less or equal to N.
 *  All rows are sorted in the database by department (if any), section header and in descending
 *  order the salaries (if any).
 *
 *  The rows are on a row by row basis formatted by a Scala match case construct
 *  according to the section tag. The resulting string is finally printed.
 */

// The main application, first definition below 
object TopNrankPureSLICK extends App {

  val topN = 3 // Number of ranking salaries.

  // The query interface for the Emp and H2 provided dual table
  val employees = TableQuery[Emp]
  val dual = TableQuery[Dual]

  // Enumerates the sections
  object Sections extends Enumeration {
    val HeadLine, TotSummary, DeptSeparator, DeptSummary, //
    DeptColNames, DeptColLineal, MainSection, BottomLine = Value
  }
  import Sections._

  /** The first part of the union all query
   *  Yields to "Report N ranks for each department."
   */
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
    (headLineQuery(topN0)
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

  /** Convert a row represented as a tuple in a formatted string.*/
  def presentation(compoundRow: (Int, String, String, Option[String], Option[Double], Int, Int)): String = {
    def fNullable(money: Option[Double]) =
      if (money.isEmpty) "null" else f"${money.getOrElse(0.0)}%9.2f"

    compoundRow match { // Formatting accordingly to the section tag "st"
      case (st, _, _, _, _, _, count) if (st == HeadLine.id) =>
        f"Report top$count%3d ranks for each department."
      case (st, _, _, _, salary, rank, count) if (st == TotSummary.id) =>
        f"Tot.$count%3d employees in$rank%2d deps.Avg. sal.:${fNullable(salary)}%9s"
      case (st, _, _, _, _, _, _) if (st == DeptSeparator.id) => "-"
      case (id, _, _, dept, salary, _, count) if (id == DeptSummary.id) =>
        f"Department:${dept.get}%5s, pop:$count%3d.Avg Salary:${salary.getOrElse(.0)}%10.2f"
      case (st, _, _, _, _, _, _) if (st == DeptColNames.id) =>
        "   Employee ID       Employee name   SalaryRank"
      case (st, _, _, _, _, _, _) if (st == DeptColLineal.id) =>
        "-------------+-------------------+--------+---+"
      case (st, idd, name, dept, salary, rank, count) if (st == MainSection.id) =>
        f"$idd%14s$name%20s${fNullable(salary)}%9s${
          (if (count > 1) "T" else "") + (if (rank > 0) rank.toString else "-")
        }%4s"
      case (st, _, _, _, _, _, count) if (st == BottomLine.id) => f"$count%4d Employees are listed."
    }
  } // presentation

  ////////////////////////// Program starts here //////////////////////////////

  // Create a connection (called a "session") to an in-memory H2 database
  // NO autocommit and some options to consider
  Database.forURL("jdbc:h2:mem:hello", driver = "org.h2.Driver").withTransaction {
    implicit session =>

      // Create the schema
      employees.ddl.create

      // Fill the database, commit work if success
      session.withTransaction {
        employees ++= Emp.content
      }

      // Precompile the composed query
      val allQueryCompiled = Compiled(allQuery(_)) // To compile the parameter must be Column[Int]
      // Test generated SQL   println(allQueryCompiled(topN).selectStatement)
      // Execute the precompiled query.
      allQueryCompiled(topN).foreach { row => println(presentation(row)) }
  } // session
} // TopNrankPureSLICK