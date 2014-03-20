package nl.lambdalloyd

import scala.slick.driver.H2Driver.simple._
import scala.slick.driver.H2Driver.simple.{ doubleColumnType, intColumnType, stringColumnType }
import scala.slick.lifted.Compiled

import scala.slick.lifted.{ Column, TableQuery }
import scala.slick.profile.RelationalDriver

import slick.driver.H2Driver.backend.Database

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
object TopNrankPureSLICK extends App with TopNrankPureSLICKtrait {

  import Sections._

  /** Convert a row represented as a tuple in a formatted string.*/
  def presentation(compoundRow: (Int, String, String, Option[String], Option[Double], Int, Int)): String = {
    def fNullable(money: Option[Double]) =
      if (money.isEmpty) "null" else f"${money.getOrElse(0.0)}%9.2f"

    import Sections._
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
        employees ++= Emp.testContent
      }

      // Precompile the composed query
      val allQueryCompiled = Compiled(allQuery(_)) // This forces the parameter must be Column[Int]
      // Test generated SQL   println(allQueryCompiled(topN).selectStatement)
      // Execute the precompiled query.
      allQueryCompiled(topN).foreach { row => println(presentation(row)) }
  } // session
} // TopNrankPureSLICK