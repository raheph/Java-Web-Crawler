import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mysql.jdbc.Statement;
 /**
  * A web crawler that collects url and words from a web page.  
  * @author Rahel Ephrem
  *
  */
public class WebCrawler {
	public static DBStoredProcedures db = new DBStoredProcedures(); 
	public static void main(String[] args) throws SQLException, IOException {
		String givenUrl = null;
		@SuppressWarnings("resource")
		Scanner in = new Scanner(System.in);
		System.out.println("Enter the URL on the form of http://example.com : ");
		givenUrl = in.nextLine();
		db.setForeignKey("SET FOREIGN_KEY_CHECKS = 0;");
		db.runSql2("TRUNCATE WEB_URLS;");
		db.runSql2("TRUNCATE URL_WORDS;");
		db.setForeignKey("SET FOREIGN_KEY_CHECKS = 1;");		
		processPage(givenUrl);
	}
	
	public static void processPage(String url) throws SQLException, IOException{		
        Document doc = Jsoup.connect(url).timeout(10*1000).get();
        Elements links = doc.select("a[href]");
        String sql;
        ResultSet rs;
        int id;

        //get the links
        for (Element link : links) {        	
        	sql = "select * from WEB_URLS where URL = '"+link.attr("abs:href")+"'";
        	rs = db.runSql(sql);      	
        	if(! rs.next()){
        		String  query = "insert into WEB_URLS (URL)" + " values (?)";
    			//store the URL to database
        		PreparedStatement stmt = db.conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        		stmt.setString(1, link.attr("abs:href"));       		
        		stmt.execute();
        		ResultSet keys = stmt.getGeneratedKeys();
        		keys.next();
        		id = keys.getInt(1);
        		processWords(link.attr("abs:href"), doc, id);
        	}
        }
	}
		
	public static void processWords(String url, Document doc, int id) throws SQLException, IOException{
		String words = doc.body().text();
		String[] breakDown = words.split(" ");
		String  query = "insert into URL_WORDS (word, Count, URLS_URLId)" + " values (?, ?, ?)";
		//build key value pair to count words
		HashMap<String, Integer> myMap = new HashMap<String, Integer>();
		if (breakDown != null) {
		for(String word: breakDown){
			if(myMap.containsKey(word)) {
				myMap.put(word, myMap.get(word) + 1);
		}  else {
			myMap.put(word,1);
		}	
		}		
		}
		 java.util.Iterator<Entry<String, Integer>> it = myMap.entrySet().iterator();
		 while(it.hasNext()) {
			 	@SuppressWarnings("rawtypes")
				Map.Entry pair = (Map.Entry)it.next();
			 	PreparedStatement stmt = db.conn.prepareStatement(query);
			 	stmt.setString(1, (String) pair.getKey());
				stmt.setInt(2, (int) pair.getValue());
				stmt.setInt(3,id);
				stmt.execute();
		 }
	}
}