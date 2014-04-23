Top rank per group
==================

Find the top N salaries in each department, where N is provided as a parameter.
Inspired by [RosettaCode.org task](http://rosettacode.org/wiki/Top_rank_per_group).

This case is expanded to take somebody with no (Null) salary and equal earning colleagues.

Sample output:
```
{{{Report top  3 ranks for each department.
Tot. 16 employees in 4 deps.Avg. sal.: 33336,67
-
Department: D050, pop:  3.Avg Salary:  34450,00
   Employee ID       Employee name   SalaryRank
-------------+-------------------+--------+---+
        E21437          John Rappl 47000,00   1
        E41298        Nathan Adams 21900,00   2
        E21438             trainee     null   -
-
Department: D101, pop:  5.Avg Salary:  33980,00
   Employee ID       Employee name   SalaryRank
-------------+-------------------+--------+---+
        E00127      George Woltman 53500,00   1
        E04242     David McClellan 41500,00   2
        E10297       Tyler Bennett 32000,00   3
-
Department: D190, pop:  4.Avg Salary:  36675,00
   Employee ID       Employee name   SalaryRank
-------------+-------------------+--------+---+
        E10001          Kim Arlich 57000,00   1
        E16399       Timothy Grave 29900,00  T2
        E16400       Timothy Grive 29900,00  T2
        E16398       Timothy Grove 29900,00  T2
-
Department: D202, pop:  4.Avg Salary:  28637,50
   Employee ID       Employee name   SalaryRank
-------------+-------------------+--------+---+
        E01234        Rich Holcomb 49500,00   1
        E39876      Claire Buckman 27800,00   2
        E27002     David Motsinger 19250,00   3
  13 Employees are listed.
```
}}}

