package nl.lambdalloyd
import org.scalatest._
import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.meta._

class TablesSuite extends FunSuite with BeforeAndAfter {

  db

  val employees = TableQuery[Emp]

  implicit var session: Session = _

  def createSchema = (employees.ddl).create

  def insertEmployee1 = employees += ("E21438", "trainee", Option("D050"), None)

  def insertEmployee2 = employees += ("E21439", "trainee", Option("D050"), None)

  def insertContent = employees ++= Emp.testContent.toIterable

  before {
    session = db.createSession
  }

  test("Creating the schema works") {
    createSchema

    val tables = MTable.getTables().list()
    assert(tables.size === 1)
    assert(tables.map(_.name.name).contains(Emp.TABLENAME))
  }

  test("Inserting an employee works") {
    createSchema

    val insertCount = insertEmployee1
    assert(insertCount === 1)
  }

  test("Query employees works") {
    createSchema
    session.withTransaction {
      insertEmployee1
    }
    session.withTransaction {
      insertEmployee2
      assert(employees.list().size === 2)
      session.rollback
    }
    val results = employees.list()
    assert(results.size === 1)
    assert(results.head === ("E21438", "trainee", Option("D050"), None))
  }

  test("DB populated works") {
    createSchema
    insertContent
    val results = employees.list().size
    assert(results === 16)
  }

  after {
    session.close()
  }
}