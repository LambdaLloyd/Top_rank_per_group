package nl.lambdalloyd

import PortableDriver.simple._

// An Employees table with 4 columns: Employee ID, Employee Name, Salary, Department,
class Emp(tag: Tag) extends Table[(String, String, Option[String], Option[BigDecimal])](tag, Emp.TABLENAME) {
  def id: Column[String] = column("EMP_ID", O.PrimaryKey) // This is the primary key column
  def name: Column[String] = column("EMP_NAME")
  def deptId: Column[Option[String]] = column("DEPT_ID")
  def salary: Column[Option[BigDecimal]] = column("SALARY", O.Nullable)

  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, name, deptId, salary)
}

object Emp {
  val TABLENAME = "EMP"

  def testContent: Seq[(String, String, Option[String], Option[BigDecimal])] =
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

  // The query interface for the Emp and H2 provided dual table
  val employees = TableQuery[Emp]

  import scala.slick.jdbc.meta.MTable

  def existsEmpTable(implicit session: Session) =
    MTable.getTables(None, PortableDriver.schema, Option(Emp.TABLENAME), Option(Seq(("TABLE")))).list.size == 1

  def insertContent(implicit session: Session,
                    freshContent: Seq[(String, String, Option[String], Option[BigDecimal])]): Option[Int] = {
    employees ++= freshContent
  }

  def conditionalCreateAndFillEmp(
    implicit session: Session,
    freshContent: Seq[(String, String, Option[String], Option[BigDecimal])] = Seq()): Int = {

    if (!existsEmpTable) {
      println(TABLENAME + " doesn't exists, so it will be created and eventually filled.")
      // Create the schema conditional
      employees.ddl.create
      if (!freshContent.isEmpty)
        // Fill the database, commit work if success
        session.withTransaction {
          insertContent(session, Emp.testContent).getOrElse(0)
        }
      else 0
    } else 0
  } // def conditionalCreateAndFillEmp

}

/*object Databases extends Enumeration {
  val Derby, H2emb, H2svr, HSQLD, Hyper, I_DB2, MsSQL, MySQL, Ora12, PSSQL, SQLit = Value
}
*/
