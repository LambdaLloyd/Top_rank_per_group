package nl.lambdalloyd

import scala.slick.driver.H2Driver.simple._

trait TopNrankPureSLICKtrait {
  val topN = 3 // Number of ranking salaries.

  // The query interface for the Emp and H2 provided dual table
  val employees = TableQuery[Emp]

  def createAndFillEmp(implicit session: Session) {
    // Create the schema
    employees.ddl.create
    session.withTransaction {
      // Fill the database, commit work if success
      employees ++= Emp.testContent
    }
  }

  // Enumerates the sections
  object Sections extends Enumeration {
    val HeadLine, TotSummary, DeptSeparator, DeptSummary, //
    DeptColNames, DeptColLineal, MainSection, BottomLine = Value
  }

  import Sections._

  /** The first part of the union all query
   *  Yields to "Report N ranks for each department."
   */
  def headLineQuery(topNo: Column[Int]) =
    Query(HeadLine.id, "", "", Option("0"), Option(0.0), 0, topNo) 
    
  /** The second part of the union all query
   *  Summarize the total table
   */
  def totSummaryQuery = Query(TotSummary.id, "", "", Option("0"),
    employees.map(_.salary).avg,
    employees.map(_.deptId).countDistinct,
    employees.map(_.id).length)

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
  def mainQuery(topNc: Column[Int]) = {
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
      row =>
        (MainSection.id,
          row.id, row.name, row.deptId, row.salary,
          countColleaguesHasMoreOrSame(row.id), /*a.k.a. rank*/
          countSalaryTies(row.deptId, row.salary))
    }.filter(_._6 /*In fact countColleaguesHasMoreOrSame*/ <= topNc)
  } // mainQuery

  //** The last part of the union all query*/
  def bottomLineQuery(topNo: Column[Int]) =
    Query(BottomLine.id, "", "", Option(None + ""), // Work around, evaluates to Some("None") mend is None
      Option(0.0), 0, mainQuery(topNo).length) // to force a last place is sorting.

  /** Compose the query with above queries and union all's*/
  def allQuery(topN0: Column[Int]) =
    (headLineQuery(topN0)
      ++ totSummaryQuery
      ++ depSepaQuery
      ++ depSummaryQuery
      ++ depColNamesQuery
      ++ depColLineal
      ++ mainQuery(topN0)
      ++ bottomLineQuery(topN0)) // Sort based on the following columns
      .sortBy(_._5.desc.nullsLast) // salary
      .sortBy(_._1) //section
      .sortBy(_._4.nullsLast) //deptId

  // Prepare the composed query 
  val allQueryCompiled = Compiled(allQuery(_)) // This forces the parameter must be Column[Int]
  // Test generated SQL     println(allQueryCompiled(topN).selectStatement)
}