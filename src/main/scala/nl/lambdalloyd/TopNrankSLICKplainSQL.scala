package nl.lambdalloyd
import scala.slick.driver.H2Driver.simple._
import scala.slick.lifted.ProvenShape
import nl.lambdalloyd._

// The main application
object TopNrankSLICKplainSQL extends App {

  val topN = 3

  // The query interface for the Emp table
  val employees = TableQuery[Emp]

  // Create a connection (called a "session") to an in-memory H2 database
  Database.forURL("jdbc:h2:mem:hello", driver = "org.h2.Driver").withTransaction {
    implicit session =>

      // Create the schema
      employees.ddl.create

      // Fill the database and commit if succeeds 
      session.withTransaction {
        employees ++= Seq(
          ("E10297", "Tyler Bennett", Option("D101"), Option(32000.0)),
          ("E21437", "John Rappl", Option("D050"), Option(47000)),
          ("E00127", "George Woltman", Option("D101"), Option(53500)),
          ("E63535", "Adam Smith", Option("D202"), Option(18000)),
          ("E39876", "Claire Buckman", Option("D202"), Option(27800)),
          ("E04242", "David McClellan", Option("D101"), Option(41500)),
          ("E01234", "Rich Holcomb", Option("D202"), Option(49500)),
          ("E41298", "Nathan Adams", Option("D050"), Option(21900)),
          ("E43128", "Richard Potter", Option("D101"), Option(15900)),
          ("E27002", "David Motsinger", Option("D202"), Option(19250)),
          ("E03033", "Tim Sampair", Option("D101"), Option(27000)),
          ("E10001", "Kim Arlich", Option("D190"), Option(57000)),
          ("E16398", "Timothy Grove", Option("D190"), Option(29900)),
          ("E16399", "Timothy Grave", Option("D190"), Option(29900)),
          ("E16400", "Timothy Grive", Option("D190"), Option(29900)))
      }
      /* Manual SQL / String Interpolation */
      // Required import for the sql interpolator
      import scala.slick.jdbc.StaticQuery.interpolation

      // Construct a SQL statement manually with an interpolated value
      val plainQuery = // First the bun
        sql"""select case LINE
         when 10 then
          'Tot.' || LPAD(POPULATION, 2) || ' Employees in ' || TIE_COUNT ||
          ' deps.Avg salary:' || TO_CHAR(SALARY, '99990.99')
         when 30 then
          '-'
         when 50 then
          'Department: ' || DEPT_ID || ', pop: ' || POPULATION ||
          '. Avg Salary: ' || TO_CHAR(SALARY, '99990.99')
         when 70 then
          LPAD('Employee ID', 14) || LPAD('Employee name', 20) ||
          LPAD('Salary', 9) || 'Rank'
         when 90 then
          LPAD('+', 14, '-') || LPAD('+', 20, '-') || LPAD('+', 9, '-') ||
          LPAD('+', 4, '-')
         else
          LPAD(' ', 8) || LPAD(EMP_ID, 6) || LPAD(EMP_NAME, 20) ||
          TO_CHAR(SALARY, '99990.99') || LPAD(case
                                                when TIE_COUNT = 1 then  ' '
                                                else 'T'
                                              end || RANK, 4)
       end "Top rank per group"
  from (select 10 LINE
              ,null EMP_ID
              ,null EMP_NAME
              ,' ' DEPT_ID
              ,avg(SALARY) SALARY
              ,0 RANK
              ,count(distinct DEPT_ID) TIE_COUNT
              ,count(*) POPULATION
          from EMP
        union all
        select 30      LINE
              ,null    EMP_ID
              ,null    EMP_NAME
              ,DEPT_ID
              ,0       SALARY
              ,0       RANK
              ,0       TIE_COUNT
              ,0       POPULATION
          from EMP
         group by DEPT_ID
        union all
        select 50 LINE
              ,null EMP_ID
              ,null EMP_NAME
              ,DEPT_ID
              ,avg(SALARY) SALARY
              ,0 RANK
              ,0 TIE_COUNT
              ,count(*) POPULATION
          from EMP
         group by DEPT_ID
        union all
        select 70      LINE
              ,null    EMP_ID
              ,null    EMP_NAME
              ,DEPT_ID
              ,0       SALARY
              ,0       RANK
              ,0       TIE_COUNT
              ,0       POPULATION
          from EMP
         group by DEPT_ID
        union all
        select 90      LINE
              ,null    EMP_ID
              ,null    EMP_NAME
              ,DEPT_ID
              ,0       SALARY
              ,0       RANK
              ,0       TIE_COUNT
              ,0       POPULATION
          from EMP
         group by DEPT_ID
        union all
        select 110 LINE
              ,EMP_ID
              ,EMP_NAME
              ,DEPT_ID
              ,SALARY
              ,(select count(distinct EMP4.SALARY)
                  from EMP EMP4
                 where EMP4.DEPT_ID = EMP3.DEPT_ID
                   and EMP4.SALARY >= EMP3.SALARY) RANK
              ,(select count(*)
                  from EMP EMP2
                 where EMP2.DEPT_ID = EMP3.DEPT_ID
                   and EMP2.SALARY = EMP3.SALARY) TIE_COUNT
              ,0 POPULATION
          from EMP EMP3
         where $topN >= -- Here is the meat, Correlated subquery
               (select count(distinct EMP4.SALARY)
                  from EMP EMP4
                 where EMP4.DEPT_ID = EMP3.DEPT_ID
                   and EMP4.SALARY >= EMP3.SALARY))
 order by DEPT_ID ,LINE ,SALARY desc, EMP_ID""".as[String]

      // Execute the query
      plainQuery.foreach(println)
  } // session
} // TopNrankSLICK
