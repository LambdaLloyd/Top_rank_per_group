package nl.lambdalloyd

import PortableDriver.simple._

import org.scalatest._

class TopNrankSLICKplainSqlSuite extends WordSpec {
  import TopNrankSLICKplainSqlSuite._

  private implicit val session = db.createSession
  
 import scala.slick.jdbc.StaticQuery.interpolation
    
val topN = 3

  // Construct a SQL statement manually with an interpolated value
  val plainQuery = // First the bun
    sql"""select case SECTION
           when 0
           then
                'Report top'
             || lpad(TIE_COUNT, 2)
             || ' ranks for each department.'
           when 1
           then
                'Tot.:'
             || lpad(TIE_COUNT, 3)
             || ' employees in'
             || lpad(rank, 2)
             || ' deps. Avg.sal.:'
             || to_char(SALARY, '999990.99')
           when 2
           then
             '-'
           when 3
           then
                'Department: '
             || DEPT_ID
             || ', pop:'
             || lpad(TIE_COUNT, 3)
             || '. Avg. Salary:'
             || to_char(SALARY, '999990.99')
           when 4
           then
                lpad('Employee ID', 14)
             || lpad('Employee name', 20)
             || lpad('Salary', 12)
             || 'Rnk'
           when 5
           then
                lpad('+', 14, '-')
             || lpad('+', 20, '-')
             || lpad('+', 12, '-')
             || lpad('+', 3, '-')
           when 6
           then
                lpad(' ', 8)
             || lpad(EMP_ID, 6)
             || lpad(EMP_NAME, 20)
             || nvl(to_char(SALARY, '99999990.99'), lpad('-', 12))
             || lpad(
                     case when TIE_COUNT > 1 then 'T' else ' ' end
                  || case when rank > 0 then to_char(rank) else '-' end,
                  3)
           when 7
           then
             lpad(TIE_COUNT, 3) || ' Employees are listed.'
         end
           "Top rank per group"
    from (select 0 SECTION,
                 '' EMP_ID,
                 '' EMP_NAME,
                 ' ' DEPT_ID,
                 0.0 SALARY,
                 0 rank,
                 $topN TIE_COUNT
            from dual
          union all
          select 1,
                 '',
                 '',
                 ' ',
                 X59,
                 X61,
                 X63
            from (select avg(SALARY) X59 from EMP),
                 (select count(distinct DEPT_ID) X61 from EMP),
                 (select count(*) X63 from EMP)
          union all
            select 2,
                   '',
                   '',
                   DEPT_ID,
                   0.0,
                   0,
                   0
              from EMP
          group by DEPT_ID
          union all
            select 3,
                   '',
                   '',
                   DEPT_ID,
                   avg(SALARY),
                   0,
                   count(*)
              from EMP
          group by DEPT_ID
          union all
            select 4,
                   '',
                   '',
                   DEPT_ID,
                   0.0,
                   0,
                   0
              from EMP
          group by DEPT_ID
          union all
            select 5,
                   '',
                   '',
                   DEPT_ID,
                   0.0,
                   0,
                   0
              from EMP
          group by DEPT_ID
          union all
          select 6,
                 EMP_ID,
                 EMP_NAME,
                 DEPT_ID,
                 SALARY,
                 COUNTCOLLEAGUESHASMOREORSAME,
                 TIE_COUNT
            from (select EMP_ID,      -- Here is the meat, Correlated subquery
                         EMP_NAME,
                         DEPT_ID,
                         SALARY,
                         (select count(distinct EMP2.SALARY)
                            from EMP EMP1, EMP EMP2
                           where     EMP0.EMP_ID = EMP1.EMP_ID
                                 and EMP1.SALARY <= EMP2.SALARY
                                 and EMP1.DEPT_ID = EMP2.DEPT_ID)
                           COUNTCOLLEAGUESHASMOREORSAME,
                         (select count(*)
                            from EMP
                           where     EMP0.DEPT_ID = EMP.DEPT_ID
                                 and EMP0.SALARY = EMP.SALARY)
                           TIE_COUNT
                    from EMP EMP0)
           where COUNTCOLLEAGUESHASMOREORSAME <= $topN
          union all
          select 7,
                 '',
                 '',
                 'None',
                 0.0,
                 0,
                 NUMBERLISTED
            from (select count(*) NUMBERLISTED
                    from (select (select count(distinct EMP2.SALARY)
                                    from EMP EMP1, EMP EMP2
                                   where     EMP0.EMP_ID = EMP1.EMP_ID
                                         and EMP1.SALARY <= EMP2.SALARY
                                         and EMP1.DEPT_ID = EMP2.DEPT_ID)
                                   rank
                            from EMP EMP0) COUNTCOLLEAGUESHASMOREORSAME
                   where rank <= $topN))
            order by DEPT_ID nulls last, SECTION, SALARY desc nulls last, EMP_NAME""".as[String]

  "The Emp table" when {
    "totally filled with default context" should {
      s"have total number of ${Emp.testContent.size} rows" in
        assert(Emp.employees.length.run === Emp.testContent.size)
    }
    
    val ist = plainQuery.buildColl[Vector]
    
    "the query is runned" should {
      "have the first row matched with expected value" in
        assert(ist === expectedRows)
      "the queryresult matches all expected rows" in assert(ist === expectedRows)
    }
  }
}

object TopNrankSLICKplainSqlSuite {

  //val db = PortableDriver.db

  val db = Database.forURL(
    "jdbc:h2:~/test;AUTOCOMMIT=OFF;WRITE_DELAY=300;MVCC=TRUE;LOCK_MODE=0",
    user = "sa",
    password = "",
    prop = null,
    driver = "org.h2.Driver")

 
  /** The expected rows after been filled with testContent and queried */
  val expectedRows = Vector(
    "Report top 3 ranks for each department.",
    "Tot.: 16 employees in 4 deps. Avg.sal.:  33336.67",
    "-",
    "Department: D050, pop:  3. Avg. Salary:  34450.00",
    "   Employee ID       Employee name      SalaryRnk",
    "-------------+-------------------+-----------+--+",
    "        E21437          John Rappl    47000.00  1",
    "        E41298        Nathan Adams    21900.00  2",
    "        E21438             trainee           -  -",
    "-",
    "Department: D101, pop:  5. Avg. Salary:  33980.00",
    "   Employee ID       Employee name      SalaryRnk",
    "-------------+-------------------+-----------+--+",
    "        E00127      George Woltman    53500.00  1",
    "        E04242     David McClellan    41500.00  2",
    "        E10297       Tyler Bennett    32000.00  3",
    "-",
    "Department: D190, pop:  4. Avg. Salary:  36675.00",
    "   Employee ID       Employee name      SalaryRnk",
    "-------------+-------------------+-----------+--+",
    "        E10001          Kim Arlich    57000.00  1",
    "        E16399       Timothy Grave    29900.00 T2",
    "        E16400       Timothy Grive    29900.00 T2",
    "        E16398       Timothy Grove    29900.00 T2",
    "-",
    "Department: D202, pop:  4. Avg. Salary:  28637.50",
    "   Employee ID       Employee name      SalaryRnk",
    "-------------+-------------------+-----------+--+",
    "        E01234        Rich Holcomb    49500.00  1",
    "        E39876      Claire Buckman    27800.00  2",
    "        E27002     David Motsinger    19250.00  3",
    " 13 Employees are listed.")
} // object TopNrankPureSLICKSuite