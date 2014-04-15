package nl.lambdalloyd
package slickeval

import org.scalatest._
import PortableDriver.simple._

trait TablesTest extends FunSuite with BeforeAndAfter {

  implicit var session: Session = _

  def insertEmployee1 = Emp.employees += ("E21438", "trainee", Option("D050"), None)

  def insertEmployee2 = Emp.employees += ("E21439", "trainee", Option("D050"), None)

  before {
    session = PortableDriver.db.createSession
  }

  test("Creating the schema works") {

    info(s"Driver is: ${PortableDriver.driverName}, detected as ${PortableDriver.detector}")
    info(s"Try to create a table, so no (${Emp.conditionalCreateAndFillEmp(session)}) rows are added")
    assert(Emp.existsEmpTable)
  }

  test("Inserting an employee works") {
    session.withTransaction { // Explicit transaction to force a commit
      Emp.employees.delete
      val insertCount = insertEmployee1
      assert(insertCount === 1)
    }
  }

  test("Quering and transactions employees works") {
    session.withTransaction {
      insertEmployee2

      assert(Emp.employees.list().size === 2)
      session.rollback
    }

    val results = Emp.employees.list()
    assert(results.size === 1)
    assert(results.head === ("E21438", "trainee", Option("D050"), None))
  }

  test("DB populated works") {
    Emp.employees.delete
    info(s"${Emp.insertContent(session, Emp.testContent).get} rows added")
    val results = Emp.employees.list().size
    assert(results === 16)
  }

  after { // Oracle automatically commits for a normally program termination.
    session.close()
  }
}