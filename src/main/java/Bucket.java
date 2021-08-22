import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Bucket implements Serializable {
	String indexName;
	String bucketNumber;
	String Name;
	Vector<Vector<Object>> bucket;
	Vector<String> overFlowBuckets;
	// int nextOverFlowNum;
	// bool overflow
	int maxEntries;

	public Bucket(String indexName, int[] bucketLocationIndex) {
		bucket = new Vector<Vector<Object>>();
		overFlowBuckets =new Vector<String>();
		this.indexName = indexName;
		maxEntries = getMaxEntriesInBucket();
		String s = "";
		for (int i = 0; i < bucketLocationIndex.length; i++) {
			s += bucketLocationIndex[i];

		}

		this.bucketNumber = s;
		this.Name = this.indexName + this.bucketNumber;
		String fileName = "src/main/resources/data/" + this.Name + ".Bucket" + ".ser";
		File myObj = new File(fileName);

		try {
			myObj.createNewFile();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public Bucket(Bucket b) {
		bucket = new Vector<Vector<Object>>();
		overFlowBuckets =new Vector<String>();
		this.indexName = b.indexName;
		maxEntries = getMaxEntriesInBucket();
		this.bucketNumber = b.bucketNumber;
		this.Name = this.indexName + this.bucketNumber + "Overflow" +b.overFlowBuckets.size();
		String fileName = "src/main/resources/data/" + this.Name + ".Bucket" + ".ser";
		File myObj = new File(fileName);

		try {
			myObj.createNewFile();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		b.overFlowBuckets.add(Name);
		
	}

	

	public int getMaxEntriesInBucket() {
		int n = 0;
		String configFilePath = "src/main/resources/";
		configFilePath = Paths.get(configFilePath, "DBApp.config").toString();
		List<String> config;
		try {
			config = Files.readAllLines(Paths.get(configFilePath));
			for (int i = 0; i < config.size(); i++) {
				if (config.get(i).toLowerCase().contains("bucket")) {

					n = Integer.parseInt(config.get(i).toString().split("=")[1].replace(" ", ""));
					// System.out.println(n);

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return n;
	}
	/*
	 * public Bucket(String indexName,String bucketNumber,int nextOverFlowNum) {
	 * //the overflow bucket this.indexName=indexName;
	 * this.bucketNumber=bucketNumber;
	 * 
	 * String fileName=
	 * "src/main/resources/data/"+this.indexName+this.bucketNumber+"."+
	 * nextOverFlowNum+".Bucket"+".ser"; File myObj = new File(fileName);
	 * 
	 * try { myObj.createNewFile();
	 * 
	 * } catch (IOException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } this.nextOverFlowNum++; }
	 */

}
