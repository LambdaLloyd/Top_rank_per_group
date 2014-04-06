package nl.lambdalloyd
import PortableDriver.simple._

// The main application
object TopNrankSLICKplainSql extends App {

  val topN = 3

  // The query interface for the Emp table
  val employees = TableQuery[Emp]

  def printJvmBanner() {
    println(s"${System.getProperty("java.runtime.name")} (build ${System.getProperty("java.runtime.version")})")
    println(s"${System.getProperty("java.vm.name")} (build ${System.getProperty("java.vm.version")}, ${System.getProperty("java.vm.info")})\n")
  }

  /* Manual SQL / String Interpolation */
  // Required import for the sql interpolator
  import scala.slick.jdbc.StaticQuery.interpolation

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

  ////////////////////////// Program starts here //////////////////////////////

  printJvmBanner()
  PortableDriver.db.withTransaction {
    implicit session =>

      // Conditional create fill the table and commit if succeeds 
      Emp.conditionalCreateAndFillEmp(session, Emp.testContent)

      // Execute the query
      plainQuery.foreach(println)
  } // session
} // TopNrankSLICKplainSql
