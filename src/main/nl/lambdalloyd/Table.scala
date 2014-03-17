package nl.lambdalloyd
import scala.slick.driver.H2Driver.simple._

// An Employees table with 4 columns: Employee ID, Employee Name, Salary, Department,
class Emp(tag: Tag) extends Table[(String, String, Option[String], Option[Double])](tag, "EMP") {
  def id: Column[String] = column("EMP_ID", O.PrimaryKey) // This is the primary key column
  def name: Column[String] = column("EMP_NAME", O.NotNull)
  def deptId: Column[Option[String]] = column("DEPT_ID", O.NotNull)
  def salary: Column[Option[Double]] = column("SALARY", O.Nullable)

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, name, deptId, salary)
}

// An auxiliary table mostly present in several databases
// The soul purpose of this (pseudo) table is to provide one single row
class Dual(tag: Tag) extends Table[String](tag, "DUAL") {
  def x: Column[String] = column("X")
  def * = x
}
