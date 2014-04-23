package nl.lambdalloyd
package slickeval

import PortableDriver.simple._

object TopNrankPureSLICK extends App with TopNrankPureSLICKtrait {
  // The main application, first definition below 
  private val topN = 3 // Number of ranking salaries.

  /** Convert a row represented as a tuple in a formatted string.*/
  private def presentation(compoundRow: (Int, String, String, Option[String], Option[BigDecimal], Int, Int)) = {
    def fNullable(money: Option[BigDecimal]) =
      if (money.isEmpty) "null" else f"${money.getOrElse(BigDecimal("0"))}%9.2f"

    import TopNrankPureSLICK.{ Sections => S }
    compoundRow match { // Formatting accordingly to the section tag "st"
      case (st, _, _, _, _, _, count) if (st == S.HeadLine.id) =>
        f"Report top$count%3d ranks for each department."
      case (st, _, _, _, salary, rank, count) if (st == S.TotSummary.id) =>
        f"Tot.$count%3d employees in$rank%2d deps.Avg. sal.:${fNullable(salary)}%9s"
      case (st, _, _, _, _, _, _) if (st == S.DeptSeparator.id) => "-"
      case (id, _, _, dept, salary, _, count) if (id == S.DeptSummary.id) =>
        f"Department:${dept.get}%5s, pop:$count%3d.Avg Salary:${fNullable(salary)}%10s"
      case (st, _, _, _, _, _, _) if (st == S.DeptColNames.id) =>
        "   Employee ID       Employee name   SalaryRank"
      case (st, _, _, _, _, _, _) if (st == S.DeptColLineal.id) =>
        "-------------+-------------------+--------+---+"
      case (st, idd, name, dept, salary, rank, count) if (st == S.MainSection.id) =>
        f"$idd%14s$name%20s${fNullable(salary)}%9s${
          (if (count > 1) "T" else "") + (if (rank > 0) rank.toString else "-")
        }%4s"
      case (st, _, _, _, _, _, count) if (st == S.BottomLine.id) => f"$count%4d Employees are listed."
    }
  } // presentation

  ////////////////////////// Program starts here //////////////////////////////

  // Inspect generated SQL  
  if (scala.util.Properties.propOrFalse("printSQL")) println(allQueryCompiled(topN).selectStatement)

  // Create a connection
  // NO autocommit and some options to consider
  PortableDriver.db.withTransaction {
    implicit session =>

      Emp.conditionalCreateAndFillEmp(session, Emp.testContent)

      // Execute the precompiled query.
      allQueryCompiled(topN).foreach { row => println(presentation(row)) }
  } // withTransaction
} // TopNrankPureSLICK