package nl.lambdalloyd

import scala.slick.driver.H2Driver.simple._

// An Employees table with 4 columns: Employee ID, Employee Name, Salary, Department,
class Emp(tag: Tag) extends Table[(String, String, Option[String], Option[Double])](tag, Emp.TABLENAME) {
  def id: Column[String] = column("EMP_ID", O.PrimaryKey) // This is the primary key column
  def name: Column[String] = column("EMP_NAME", O.NotNull)
  def deptId: Column[Option[String]] = column("DEPT_ID", O.NotNull)
  def salary: Column[Option[Double]] = column("SALARY", O.Nullable)

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, name, deptId, salary)
}

object Emp {
  val TABLENAME = "EMP"

  def testContent: Seq[(String, String, Option[String], Option[Double])] =
    Seq(("E10297", "Tyler Bennett", Option("D101"), Option(32000)),
      ("E21437", "John Rappl", Option("D050"), Option(47000)),
      ("E21438", "trainee", Option("D050"), None),
      ("E00127", "George Woltman", Option("D101"), Option(53500)),
      ("E63535", "Adam Smith", Option("D202"), Option(18000.0)),
      ("E39876", "Claire Buckman", Option("D202"), Option(27800.0)),
      ("E04242", "David McClellan", Option("D101"), Option(41500.0)),
      ("E01234", "Rich Holcomb", Option("D202"), Option(49500.0)),
      ("E41298", "Nathan Adams", Option("D050"), Option(21900.0)),
      ("E43128", "Richard Potter", Option("D101"), Option(15900.0)),
      ("E27002", "David Motsinger", Option("D202"), Option(19250.0)),
      ("E03033", "Tim Sampair", Option("D101"), Option(27000.0)),
      ("E10001", "Kim Arlich", Option("D190"), Option(57000.0)),
      ("E16398", "Timothy Grove", Option("D190"), Option(29900.0)),
      ("E16399", "Timothy Grave", Option("D190"), Option(29900.0)),
      ("E16400", "Timothy Grive", Option("D190"), Option(29900.0)))
}

// An auxiliary table mostly present in several databases
// The soul purpose of this (pseudo) table is to provide one single row
class Dual(tag: Tag) extends Table[String](tag, "DUAL") {
  def x: Column[String] = column("X")
  def * = x
}

