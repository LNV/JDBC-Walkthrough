import java.sql.*;


public class Walkthrough {
	
	private static String dbName = "MAI_LNV";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connection conn = null;
		// Попытка подключения к БД
		try {
			conn = DriverManager.getConnection("jdbc:derby://localhost:1527/" + dbName);
		} catch (SQLException e) {
			// Если БД нет, то создаём её
			if (e.getErrorCode() == 40000) createDB();
		}
		// Повторная попытка подключения к БД
		try {
			conn = DriverManager.getConnection("jdbc:derby://localhost:1527/" + dbName);
			getWithStatement(conn);
			getWithPreparedStatement(conn);
			transactionalUpdates(conn);
		} catch (SQLException e) {
			printSQLError(e);
		}					
	}
	
	// Вывод данных при помощи Statement
	private static void getWithStatement(Connection conn) {
		Statement stmt = null;
		ResultSet res = null;
		String query = "SELECT * from Employee";		
		try {
			stmt = conn.createStatement();
			res = stmt.executeQuery(query);
			while (res.next()) {
				System.out.println(res.getInt("EMPNO") + ", " + res.getString("ENAME") + ", " + res.getString("JOB_TITLE"));
			}
		} catch (SQLException e) {
			printSQLError(e);			
		} finally {
			try {
				// Один catch потому, что если ошибка связана с stmt
				// то res не был инициализирован
				stmt.close();
				res.close();				
			} catch (SQLException e) {
				printSQLError(e);
			}			
		}	
		System.out.println();
				
	}
	
	// Вывод данных при помощи PreparedStatement
	private static void getWithPreparedStatement(Connection conn) {
		PreparedStatement pStmt = null;
		ResultSet res = null;
		int[] employeesID = {7369,7521};
		String query1 = "SELECT * from Employee WHERE EMPNO=?";	
		try {
			pStmt = conn.prepareStatement(query1);
			for (int i=0; i<employeesID.length; i++) {
				pStmt.setInt(1, employeesID[i]);
				res = pStmt.executeQuery();
				if (res.next())
					System.out.println(res.getInt("EMPNO") + ", " + res.getString("ENAME") + ", " + res.getString("JOB_TITLE"));
			}
		} catch (SQLException e) {
			printSQLError(e);
		} finally {
			// Один catch потому, что если ошибка связана с stmt
			// то res не был инициализирован
			try {
				pStmt.close();
				res.close();
			} catch (SQLException e) {
				printSQLError(e);
			}
		}		
		System.out.println();
	}
	
	private static void transactionalUpdates(Connection conn) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			conn.setAutoCommit(false);			
			stmt.addBatch("INSERT INTO Employee values (6666,'Some Name', 'Some Job')");
			stmt.addBatch("INSERT INTO Address values (6666,'Some Address')");
			stmt.executeBatch();			
			conn.commit();
			System.out.println("Данные были успешно внесены в БД");
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				printSQLError(e);
			}
			printSQLError(e);			
		} finally {
			try {
				stmt.close();			
			} catch (SQLException e) {
				printSQLError(e);
			}			
		}	
		System.out.println();
	}
	
	
	// Трассировка ошибки
	private static void printSQLError(SQLException e) {
		System.out.println("SQLError: " + e.getMessage() + " code: " + e.getErrorCode());
	}
	
	// Создание БД МАИ
	private static void createDB() {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = DriverManager.getConnection("jdbc:derby://localhost:1527/" + dbName + ";create=true");
			conn.setAutoCommit(false);	
			stmt = conn.createStatement();
			stmt.addBatch("CREATE TABLE Employee (EMPNO int NOT NULL, ENAME varchar(50) NOT NULL, JOB_TITLE varchar(150) NOT NULL)");			
			stmt.addBatch("INSERT INTO Employee values (7369,'John Smith', 'Clerk'), (7499,'Joe Allen','Salesman'), (7521,'Mary Lou','Director')");	
			stmt.addBatch("CREATE TABLE Address (EMPNO int NOT NULL, HOME_ADDRESS varchar(200) NOT NULL)");
			stmt.executeBatch();			
			conn.commit();
		} catch (SQLException e) {
			printSQLError(e);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				printSQLError(e);
			}
			System.out.println("Ошибка создания БД " + dbName);
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				printSQLError(e);
			}
		}
	}


}
