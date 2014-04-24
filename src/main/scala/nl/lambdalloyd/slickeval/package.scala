package nl.lambdalloyd
/** Provides executables for running the Top rank per group problem, finding the top-n
 *  salaries in each department, where n is provided as a parameter.
 *
 *  ==Overview==
 *
 *  There are two solutions made:
 *
 *  - TopNrankPureSLICK - a solution made in Scala-language
 *  - TopNrankSLICKplainSql - which passing through plain SQL
 *
 *  How it works:
 *
 *  One composed query produces all the rows necessary for the output. The composed query is
 *  made of a union (union all) of queries mostly getting a row per department. Two queries
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
 *
 *  @version			0.92	2014-04-04
 *  @author		Frans W. van den Berg
 */
package object slickeval
