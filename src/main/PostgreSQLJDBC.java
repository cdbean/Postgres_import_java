package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PostgreSQLJDBC {
   public static void main(String args[]) {
	  PostgreSQLJDBC jdbc = new PostgreSQLJDBC();

      List<Map<String, String>> data = new ArrayList<Map<String, String>>();
      Map<String, String> d = new HashMap<String, String>();
      d.put("entity", "person");
      d.put("name", "jackie");
      d.put("id_ori", "234");
      data.add(d);
      
      jdbc.ImportData(data);
      
   }
   
   public void ImportData(List<Map<String, String>> data) {
	   Connection c = null;
       try {
	      c = this.BuildConnection(c);
	      c.setAutoCommit(false);
       } catch (SQLException e1) {
			e1.printStackTrace();
	   }  
       this.CreateTable(c);
       this.AddData(c,  data);
       this.CloseConnection(c);
   }
   
   public void CreateTable(Connection c) {
	   List<String> tables = Arrays.asList("person", "event", "location", "facility", "vehicle", "organization", "event_person", "event_location", "event_organization", "event_facility", "event_vehicle", "person_organization");
	   List<String> toAddTables = new ArrayList<String>();
	   
	   try {
		   DatabaseMetaData dbm = c.getMetaData();
		   // check if table is there
		   for (String t : tables) {
			   ResultSet res = dbm.getTables(null, null, t, null);
			   if (! res.next()) {
				   toAddTables.add(t);
			   }
		   }
		} catch (SQLException e1) {
			e1.printStackTrace();
		}  
	   try {
		   Statement stmt = null;
		   stmt = c.createStatement();
		   String sql = null;
		   for (String t : toAddTables) {
			   if (t == "person") {
				   System.out.println("Creating Table Person ...");
			       sql = "CREATE TABLE Person " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " name           CHAR(50),  " +
			                    " alias          CHAR(50),  " +
			                    " section        CHAR(50),  " +
			                    " region         CHAR(50), " +
			                    " role           CHAR(50)," +
			                    " prof           CHAR(50),  " +
			                    " living           BOOLEAN,  " +
			                    " remark           CHAR(100),  " +
			                    " age            INT      , " +
			                    " types          CHAR(100), " +
			                    " pedigree         CHAR(50),"  +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event") {
				   System.out.println("Creating Table Event ...");
			       sql = "CREATE TABLE Event " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
		                        " types           CHAR(100)," +
			                    " date        CHAR(10),  " +
			                    " remark         CHAR(100), " +
			                    " pedigree           CHAR(50)," +
			                    " descr          CHAR(100),  " +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "location") {
				   System.out.println("Creating Table Location ...");
			       sql = "CREATE TABLE location " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " descr           CHAR(50),  " +
			                    " types          CHAR(50),  " +
			                    " precision        REAL,  " +
			                    " remark         CHAR(100), " +
			                    " shape           GEOMETRY," +			                    
			                    " pedigree         CHAR(50),"  +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "organization") {
				   System.out.println("Creating Table Organization ...");
			       sql = "CREATE TABLE organization " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " name           CHAR(50),  " +
			                    " descr 			 CHAR(100), " +
			                    " remark           CHAR(100),  " +
			                    " types          CHAR(100), " +
			                    " pedigree         CHAR(50),"  +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "facility") {
				   System.out.println("Creating Table Facility ...");
			       sql = "CREATE TABLE facility " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " descr           CHAR(100),  " +
			                    " remark           CHAR(100),  " +
			                    " types          CHAR(100), " +
			                    " pedigree         CHAR(50),"  +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "vehicle") {
				   System.out.println("Creating Table Vehicle ...");
			       sql = "CREATE TABLE vehicle " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " name           CHAR(50),  " +
			                    " remark           CHAR(100),  " +
			                    " types          CHAR(100), " +
			                    " descr           CHAR(100),  " +
			                    " pedigree         CHAR(50),"  +
			                    " node_text          CHAR(50), " +
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event_person") {
				   System.out.println("Creating Table event_person ...");
			       sql = "CREATE TABLE event_person " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " event_id          INTEGER REFERENCES event,  " +
			                    " person_id         INTEGER REFERENCES person,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER , " +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event_location") {
				   System.out.println("Creating Table event_location ...");
			       sql = "CREATE TABLE event_location " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " event_id          INTEGER REFERENCES event,  " +
			                    " location_id         INTEGER REFERENCES location,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER  ," +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event_organization") {
				   System.out.println("Creating Table event_organization ...");
			       sql = "CREATE TABLE event_organization " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " event_id          INTEGER REFERENCES event,  " +
			                    " organization_id         INTEGER REFERENCES organization,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER , " +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event_facility") {
				   System.out.println("Creating Table event_facility ...");
			       sql = "CREATE TABLE event_facility " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " event_id          INTEGER REFERENCES event,  " +
			                    " facility_id         INTEGER REFERENCES facility,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER , " +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "event_vehicle") {
				   System.out.println("Creating Table event_vehicle ...");
			       sql = "CREATE TABLE event_vehicle " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " event_id          INTEGER REFERENCES event,  " +
			                    " vehicle_id         INTEGER REFERENCES vehicle,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER , " +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			   else if (t == "person_organization") {
				   System.out.println("Creating Table person_organization ...");
			       sql = "CREATE TABLE person_organization " +
			                    "(id SERIAL PRIMARY KEY     NOT NULL," +
			                    " person_id          INTEGER REFERENCES person,  " +
			                    " organization_id         INTEGER REFERENCES organization,  " +
			                    " types          CHAR(100), " +
			                    " direction      INTEGER,  " +			        
			                    " id_ori          CHAR(10)  NOT NULL, " +
			                    " source          CHAR(10)) " ;
			   }
			 
			   if (sql != null) {
				   stmt.executeUpdate(sql);
			   }
			   System.out.println("Done.");
		   }  
		   c.commit();
	       stmt.close();
		   
	   } catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
       }
   }
   
   public void AddData(Connection c, List<Map<String, String>> data) {
	   try {
		   Statement stmt = null;
	   
		   stmt = c.createStatement();
		   String sql = null;
		   ListIterator litr = data.listIterator();
		   int count = 0;
		   System.out.println("Inserting records...");
		   while(litr.hasNext()) {
			   Map<String, String> d = (Map<String, String>)litr.next();
			   String entity = d.get("entity");
			   if (entity == "person" ) {
				   sql = "INSERT INTO person (name, alias, section, region, role, prof, living, remark, age, types, pedigree, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%b', '%s', %d, '%s', '%s', '%s', '%s', '%s');", d.get("name"), d.get("alias"), d.get("section"), d.get("region"), d.get("role"), d.get("prof"), d.get("living"), d.get("remark"), d.get("age"), d.get("types"), d.get("pedigree"), d.get("node_text"), d.get("id_ori"), d.get("source")); 
			   }
			   else if (entity == "event"){
				   sql = "INSERT INTO event (types, date, remark, pedigree, descr, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", d.get("name"), d.get("types"), d.get("date"), d.get("remark"), d.get("pedigree"), d.get("descr"), d.get("node_text"), d.get("id_ori"), d.get("source")); 
			   }
			   else if (entity == "location") { // the way to insert geometry may not be correct
				   sql = "INSERT INTO location (types, precision, remark, shape, descr, pedigree, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', GeometryFromText('%s',4326), '%s', '%s', '%s', '%s', '%s');", d.get("types"), d.get("precision"), d.get("remark"), d.get("shape"), d.get("descr"), d.get("pedigree"), d.get("node_text"), d.get("id_ori"), d.get("source")); 
			   }
			   else if (entity == "organization") {
				   sql = "INSERT INTO organization (types, remark, descr, pedigree, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s');", d.get("types"), d.get("remark"), d.get("descr"), d.get("pedigree"), d.get("node_text"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "facility") {
				   sql = "INSERT INTO facility (types, remark, descr, pedigree, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s');", d.get("types"), d.get("remark"), d.get("descr"), d.get("pedigree"), d.get("node_text"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "vehicle") {
				   sql = "INSERT INTO organization (types, remark, descr, pedigree, node_text, id_ori, source) "
			               + String.format("VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s');", d.get("types"), d.get("remark"), d.get("descr"), d.get("pedigree"), d.get("node_text"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "event_person") {
				   sql = "INSERT INTO event_person (event_id, person_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("event_id"), d.get("person_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "event_location") {
				   sql = "INSERT INTO event_location (event_id, location_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("event_id"), d.get("location_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "event_organization") {
				   sql = "INSERT INTO event_organization (event_id, organization_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("event_id"), d.get("organization_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "event_facility") {
				   sql = "INSERT INTO event_facility (event_id, facility_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("event_id"), d.get("facility_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "event_vehicle") {
				   sql = "INSERT INTO event_vehicle (event_id, vehicle_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("event_id"), d.get("vehicle_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   else if (entity == "person_organization") {
				   sql = "INSERT INTO person_organization (person_id, organization_id, types, direction, id_ori, source) "
			               + String.format("VALUES(%d, %d, '%s', %d, '%s', '%s', '%s');", d.get("person_id"), d.get("organization_id"), d.get("types"), d.get("direction"), d.get("id_ori"), d.get("source"));
			   }
			   		   
			   stmt.executeUpdate(sql);
			   count = count + 1;
		   }		 
		   System.out.println(String.format("Inserted %d records in total", count));
		   stmt.close();
		   c.commit();
	   } catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
       }
   }
   
   public Connection BuildConnection(Connection c) {
	   try {
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager
	            .getConnection("jdbc:postgresql://localhost:5432/test", // database
	            "postgres", "asdf1234");  // username and password
	      } catch (Exception e) {
	         e.printStackTrace();
	         System.err.println(e.getClass().getName()+": "+e.getMessage());
	         System.exit(0);
	      }
	   System.out.println("Opened database successfully");   
	   return c;
   }
   
   public void CloseConnection(Connection c) {
	   try {
		   c.close();	
		   System.out.println("Close Database");
	   } catch (Exception e) {
		   System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	       System.exit(0);
	   }
   }
}
