import simpledb.remote.SimpleDriver;

import java.sql.*;

/**
 * Created by Kevin O'Brien on 3/21/2018.
 */
public class CreateEmployeeDB {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            // Create the Department table
            String s = "create table DEPARTMENT(DId int, DName varchar(30))";
            stmt.executeUpdate(s);
            System.out.println("Table DEPARTMENT created.");

            s = "insert into DEPARTMENT(DId, DName) values ";
            String[] deptvals = {"(10, 'frontend')",
                    "(20, 'backend')",
                    "(30, 'hr')",
                    "(40, 'customerrelations')"};
            for (int i = 0; i < deptvals.length; i++)
                stmt.executeUpdate(s + deptvals[i]);
            System.out.println("DEPARTMENT records inserted.");

            // Create the employee table
            s = "create table EMPLOYEE(EId int, EName varchar(10), BirthYear int, Salary int, DeptId int)";
            stmt.executeUpdate(s);
            System.out.println("Table EMPLOYEE created.");

            s = "insert into EMPLOYEE(EId, EName, BirthYear, Salary, DeptId) values ";
            String[] studvals = {"(1, 'joe', 1970, 90000, 10)",
                    "(2, 'jim', 1985, 100000, 20)",
                    "(3, 'bob', 1945, 75000, 30)",
                    "(4, 'sue', 1990, 80000, 40)",
                    "(5, 'mark', 1950, 125000, 10)",
                    "(6, 'carol', 1940, 110000, 20)",
                    "(7, 'john', 1955, 90000, 40)",
                    "(8, 'anne', 1943, 120000, 30)",
                    "(9, 'bill', 1952, 85000, 30)"};
            for (int i = 0; i < studvals.length; i++)
                stmt.executeUpdate(s + studvals[i]);
            System.out.println("EMPLOYEE records inserted.");

            // Find all employee names and salaries in HR department
            String qry = "select Ename, Salary "
                    + "from employee, department "
                    + "where did = deptid "
                    + "and dname = 'hr'";
            ResultSet rs = stmt.executeQuery(qry);

            while (rs.next()) {
                String ename = rs.getString("ename");
                int salary = rs.getInt("salary");
                System.out.println(ename + "\t" + salary);
            }
            rs.close();

            // Find employees born in 1970
            qry = "select Ename, birthyear "
                    + "from employee "
                    + "where birthyear = 1970";
            rs = stmt.executeQuery(qry);

            while (rs.next()) {
                String ename = rs.getString("ename");
                int birthyear = rs.getInt("birthyear");
                System.out.println(ename + "\t" + birthyear);
            }
            rs.close();

            // Find employees making exactly $100,000/year and their departments
            qry = "select Ename, dname "
                    + "from employee, department "
                    + "where did = deptid "
                    + "and salary = 100000";
            rs = stmt.executeQuery(qry);

            while (rs.next()) {
                String ename = rs.getString("ename");
                String dname = rs.getString("dname");
                System.out.println(ename + "\t" + dname);
            }
            rs.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}