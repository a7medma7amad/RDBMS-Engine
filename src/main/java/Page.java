import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable {
	Vector<Hashtable<String, Object>> page;
	 String tableName;
	 int pageId;
	 Hashtable<String, Object> min;
	 Hashtable<String, Object> max;
	 
	
	public Page(String tableName,int pageId) {
		this.tableName=tableName;
		this.pageId=pageId;
		page= new Vector<Hashtable<String, Object>>();
		String fileName= "src/main/resources/data/"+tableName+"."+pageId+".ser";
		File myObj = new File(fileName);
		
		try {
			myObj.createNewFile();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public Page() {
		
	}
	public void deletePageFile() {
		String fileName= "src/main/resources/data/"+tableName+"."+pageId+".ser";
		File myObj = new File(fileName);
		myObj.delete();
	}
	
	
	
}
