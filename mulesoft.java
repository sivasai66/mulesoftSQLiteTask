import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class mulesoft {
	
	static int operations = 0, errors = 0;
	
	public static void addOperations() {
		++operations;
	}
	
	public static void addErrors() {
		++errors;
	}
	
	public static void createNewDatabase(String new_db) {
		addOperations();
		String url = "jdbc:sqlite:" + new_db + ".db";
		try {
			Connection new_con = DriverManager.getConnection(url);
            if (new_con != null) {
                System.out.println("A new database has been created.\n\n");
            }
            new_con.close();
        }
        catch (SQLException e) {
        	addErrors();
        	System.out.println(e.getMessage());
        }
	}
	
	public static void createTable() {
		addOperations();
		try {
			Connection con = DriverManager.getConnection("jdbc:sqlite:mulesoftInternship.db");
			Statement statement = con.createStatement();
	        statement.setQueryTimeout(30);
	        
	        String query = "CREATE TABLE IF NOT EXISTS movies(id integer PRIMARY KEY AUTOINCREMENT, name text NOT NULL, movie text NOT NULL, UNIQUE(name, movie));";
	        statement.execute(query);
	        System.out.println("The Table has been created.\n\n");
		}
		catch(Exception e) {
			addErrors();
			System.out.println(e.getMessage());
		}
	}
	
	public static void insertData(String name, String movie) {
		addOperations();
		try {
			Connection con = DriverManager.getConnection("jdbc:sqlite:mulesoftInternship.db");
			Statement statement = con.createStatement();
	        statement.setQueryTimeout(30);
	        String query = "INSERT INTO movies(name, movie) VALUES(?,?)";
	        PreparedStatement pstmt = con.prepareStatement(query);
	        pstmt.setString(1, name);
	        pstmt.setString(2, movie);
	        pstmt.executeUpdate();        
		}
		catch(Exception e) {
			addErrors();
			System.out.println(e.getMessage());
		}
	}
	
	public static void retrieveData(String name) {
		addOperations();
		try {
			Connection con = DriverManager.getConnection("jdbc:sqlite:mulesoftInternship.db");
			Statement statement = con.createStatement();
	        statement.setQueryTimeout(30);
	        String query = "SELECT name, movie FROM movies";
	        if(name != null) {
	        	query = query + " where name='" + name + "'";
	        }
	        ResultSet rs = statement.executeQuery(query);
	        String format = "%-20s-\t%s\n";
	        System.out.printf(format, "Artist Name", "Acted Movie");
	        while (rs.next()) {
	        	System.out.printf(format,rs.getString(1),rs.getString(2));
	        }		
		}
		catch(Exception e) {
			addErrors();
			System.out.println(e.getMessage());
		}
		System.out.println("\n\n");
	}
	
	public static void main(String[] args) {
		try {
			Class.forName("org.sqlite.JDBC");
			// Database Connection Section
			Connection con = DriverManager.getConnection("jdbc:sqlite:mulesoftInternship.db");
            System.out.println("SQLite Database Connected successfully\n\n");
            
            // Database Creation Section
            createNewDatabase("sivasai");
            
            
            // Table Creation Section
            createTable();
            
            
            // Data Insertion Section
            insertData("abhinav pamarthi", "cacd peer mentor");
            insertData("abhinav pamarthi", "sea president");
            insertData("sivasai yarlagadda", "sea member");
            insertData("sumanth summy", "cdd peer lead");
            insertData("daivakeshwar", "sea member");
            insertData("sivasai yarlagadda", "shhh magic idhi");
            insertData("abhinav pamarthi", "nis peer lead");
            insertData("sastry", "secret");
            insertData("sivasai yarlagadda", "cacd peer mentor");
            insertData("harish pendyala", "ayomayam jagannadham");
            System.out.println("Data Inserted Into The Table.\n\n");
            
            
            // Data Querying Section
            System.out.println("Querying Details:");
            retrieveData(null);
            
            
            System.out.println("Querying Details With A Particular Actor Name:");
            retrieveData("sivasai yarlagadda");
            
            System.out.println("Out Of \""+ operations +"\" Operations You Have Came Across \""+ errors + "\" Errors");
            System.out.println("-------------------------------------------------------------");
            
            con.close();
		}
		catch(Exception e) {
			e.getMessage();
		}
		
	}
	
}
