import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Table  {
	String name;
	Vector<Integer> pages;
	Vector<String> indices;
	int maxRowNumPerPage;
	int lastPageId;
	
	
	
	public Table() {
		maxRowNumPerPage = getMaxRowNumPerPage();
		lastPageId = 0;
		pages = new Vector<Integer>();
		
	}

	public Table(String n) {
		indices=new Vector<String>();
		pages = new Vector<Integer>();
		name = n;
		maxRowNumPerPage = getMaxRowNumPerPage();
		lastPageId = 0;
		
	}
	

	public int getMaxRowNumPerPage() {
		int n=0;
		String configFilePath = "src/main/resources/";
        configFilePath = Paths.get(configFilePath, "DBApp.config").toString();
        List<String> config;
		try {
			config = Files.readAllLines(Paths.get(configFilePath));
	        for (int i = 0; i < config.size(); i++) {
	            if (config.get(i).toLowerCase().contains("page")) {
	                
	                n= Integer.parseInt(config.get(i).toString().split("=")[1].replace(" ", ""));
	                //System.out.println(n);
	                
	               
	            }
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return n;
		
	}
	
}
