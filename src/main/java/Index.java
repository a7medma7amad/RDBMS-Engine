import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Index implements Serializable{
	String tableName;
	String[] columnNames;
	Object[] gridIndexArray;
	String Name;
	
	
	public Index(Table t,String[] columnNames,Object[] gridIndexArray) {
		this.columnNames=columnNames;
		this.gridIndexArray=gridIndexArray;
		this.tableName=t.name;
		
		String s="";
		for(int i=0;i<columnNames.length;i++) {
			s+=columnNames[i];
			if(i<columnNames.length-1)
				s+="_";
		}
		this.Name=tableName+"."+s;
		String fileName= "src/main/resources/data/"+this.Name+".Index"+".ser";
		File myObj = new File(fileName);
		
		try {
			myObj.createNewFile();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		t.indices.add(Name);
	}
	
	
	
}
