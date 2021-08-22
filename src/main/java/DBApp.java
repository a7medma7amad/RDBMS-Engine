import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class DBApp implements DBAppInterface, java.io.Serializable {
	transient File metadata = new File("src/main/resources/metadata.csv");
	transient Vector<Table> tables;
	private Object o;

	@Override
	public void init() {
		// TODO Auto-generated method stub
		tables = new Vector<Table>();

		Vector<String> tableNames = new Vector<String>();
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader(metadata));
			String row;

			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (tableNames.isEmpty()) {
					tableNames.add(data[0]);
				} else {
					boolean flag = true;
					for (int i = 0; i < tableNames.size(); i++) {
						if (tableNames.get(i).equals(data[0])) {
							flag = false;
						}
					}
					if (flag) {
						tableNames.add(data[0]);
					}
				}

			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int j = 0; j < tableNames.size(); j++) {
			Table t = new Table(tableNames.get(j));
			tables.add(t);

		}

		for (int k = 0; k < tables.size(); k++) {
			boolean valid = true;
			int pageId = 0;
			while (valid) {

				String fileName = "src/main/resources/data/" + tables.get(k).name + "." + pageId + ".ser";
				File f = new File(fileName);
				if (f.exists() && !f.isDirectory()) {

					tables.get(k).pages.add(pageId);

				} else {
					valid = false;
				}
				if (valid)
					pageId++;
				this.tables.get(k).lastPageId = pageId;
			}

		}
		File folder = new File("src/main/resources/data/");
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String tmp = listOfFiles[i].getName();
				if (tmp.contains("Index")) {
					tmp = tmp.replace('.', ' ');
					String[] s = tmp.split(" ");
					String indexName = s[0] + "." + s[1];
					int indexOfTable = getIndexOfTable(s[0].toLowerCase());
					tables.get(indexOfTable).indices.add(indexName);
				}
			} else if (listOfFiles[i].isDirectory()) {
				// System.out.println("Directory " + listOfFiles[i].getName());
			}
		}

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		// TODO Auto-generated method stub

		boolean valid = true;

		String row;

		BufferedReader csvReader = null;

		try {
			csvReader = new BufferedReader(new FileReader(metadata));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					valid = false;
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (valid) {
			Table t = new Table(tableName);
			tables.add(t);
			try {
				@SuppressWarnings("resource")

				FileWriter fw = new FileWriter(metadata, true);
				BufferedWriter csvWriter = new BufferedWriter(fw);
				for (int i = 0; i < colNameType.size(); i++) {

					String colName = (colNameType.entrySet().toArray())[i].toString().split("=")[0].toString();
					StringBuilder sb = new StringBuilder();
					sb.append(tableName);
					sb.append(",");
					sb.append(colName);
					sb.append(",");
					sb.append(colNameType.get(colName));
					sb.append(",");
					if (clusteringKey.equals(colName))
						sb.append("true");
					else
						sb.append("false");
					sb.append(",");
					sb.append("false");
					sb.append(",");
					sb.append(colNameMin.get(colName));
					sb.append(",");
					sb.append(colNameMax.get(colName));
					sb.append(",");
					sb.append("\n");
					csvWriter.write(sb.toString());
					csvWriter.flush();

				}
				csvWriter.close();
				// System.out.println("done");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("didnt");
				e.printStackTrace();
			}
		} else {
			System.out.println("table already exists");
		}

	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub
		Object[] temp = new Object[10];
		Object[] gridIndexArray = createNDimArray(temp, columnNames.length);
		String s = "";
		for (int i = 0; i < columnNames.length; i++) {
			s += columnNames[i];
			if (i < columnNames.length - 1)
				s += "_";
		}
		String Name = tableName + "." + s;

		for (int i = 0; i < tables.get(getIndexOfTable(tableName)).indices.size(); i++) {

			if ((tables.get(getIndexOfTable(tableName)).indices.get(i)).equals(Name)) {
				throw new DBAppException("index already is created");
			}
		}
		Table t = tables.get(getIndexOfTable(tableName));
		Index x = new Index(t, columnNames, gridIndexArray);

		if (!(tables.get(getIndexOfTable(tableName)).pages.isEmpty())) {
			// insert every row in table in this index

			int indexOfTable = getIndexOfTable(tableName);
			for (int i = 0; i < tables.get(indexOfTable).pages.size(); i++) {
				Page p = deserializePage(tableName, i);
				for (int j = 0; j < p.page.size(); j++) {
					insertIntoIndex(x, p.page.get(j), i);
				}

				serializePage(p);
			}
		}
		serializeIndex(x);
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		boolean checkType = true;
		boolean checkMinMax = true;

		checkType = checkType(tableName, colNameValue);
		checkMinMax = checkMinMax(tableName, colNameValue);

		if (checkMinMax && checkType) {
			int indexOfTable = getIndexOfTable(tableName);
			if (indexOfTable >= 0) {
				if (tables.get(indexOfTable).pages.isEmpty()) {
					Page p = new Page(tableName, tables.get(indexOfTable).lastPageId);
					p.page.add(colNameValue);
					serializePage(p);
					tables.get(indexOfTable).pages.add(tables.get(indexOfTable).lastPageId);
					Table t = tables.get(indexOfTable);
					tables.get(indexOfTable).lastPageId++;
					for (int i = 0; i < tables.get(indexOfTable).indices.size(); i++) {
						Index x = deserializeIndex(tables.get(indexOfTable).indices.get(i));
						insertIntoIndex(x, colNameValue, 0);
						serializeIndex(x);
					}
				} else {
					// at least one page
					int lastNonNullPageIndex = tables.get(indexOfTable).pages.size() - 1;
					// get last non null page index
					for (int i = 0; i < tables.get(indexOfTable).pages.size(); i++) {

						if (tables.get(indexOfTable).pages.get(tables.get(indexOfTable).pages.size() - 1 - i) != null) {
							// deserializePage(tableName, tables.get(indexOfTable).pages.size() - 1 - i);
							lastNonNullPageIndex = tables.get(indexOfTable).pages.size() - 1 - i;
							i += tables.get(indexOfTable).pages.size() + 1;
							break;
						}
					}
					// add page if the last one is full
					Page lastNonNullPage = deserializePage(tableName, lastNonNullPageIndex);
					if (lastNonNullPage.page.size() >= tables.get(indexOfTable).getMaxRowNumPerPage()) {
						serializePage(lastNonNullPage);
						Page p = new Page(tableName, tables.get(indexOfTable).lastPageId);
						serializePage(p);
						tables.get(indexOfTable).pages.add(tables.get(indexOfTable).lastPageId);
						tables.get(indexOfTable).lastPageId++;

					}

					int insertPageIndex = getIndexOfTargetPage(tableName, colNameValue); // the index of page that we
																							// want
																							// to insert in

					int currentInsertPage = insertPageIndex;
					Hashtable<String, Object> colNameValueInsert = (Hashtable<String, Object>) colNameValue.clone();

					// insert into indices
					for (int i = 0; i < tables.get(indexOfTable).indices.size(); i++) {
						Index x = deserializeIndex(tables.get(indexOfTable).indices.get(i));
						insertIntoIndex(x, colNameValueInsert, currentInsertPage);
						serializeIndex(x);
					}
					// insert into table
					for (int j = 0; j < tables.get(indexOfTable).pages.size(); j++) {

						if (tables.get(indexOfTable).pages.get(currentInsertPage) != null) {
							Page p = deserializePage(tableName, currentInsertPage);

							insertintoSortedPage(tableName, p, colNameValueInsert);

							if (p.page.size() > tables.get(indexOfTable).getMaxRowNumPerPage()) {

								colNameValueInsert = (Hashtable<String, Object>) p.page.get(p.page.size() - 1).clone();
								p.page.remove(p.page.size() - 1);
								currentInsertPage++;
								serializePage(p);

							} else {

								serializePage(p);
								j += tables.get(indexOfTable).pages.size() + 1;
							}
						} else {
							System.out.println("2nd else");
							currentInsertPage++;
						}
						// System.out.println(j);
					}

				}
			}
		}

	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		// TODO Auto-generated method stub

		boolean checkType = true;
		boolean checkMinMax = true;
		String clusteringKeyType = getClusteringKeyType(tableName);
		Object values = clusteringKeyValue;
		if (clusteringKeyType.equals("java.lang.Integer")) {
			values = Integer.parseInt(clusteringKeyValue);

		} else if (clusteringKeyType.equals("java.lang.Double")) {
			values = Double.parseDouble(clusteringKeyValue);

		}

		else if (clusteringKeyType.equals("java.lang.String")) {
			values = clusteringKeyValue;

		} else if (clusteringKeyType.equals("java.lang.Date") || clusteringKeyType.equals("java.util.Date")) {
			int year1 = Integer.parseInt(clusteringKeyValue.trim().substring(0, 4));
			int month1 = Integer.parseInt(clusteringKeyValue.trim().substring(5, 7));
			int day1 = Integer.parseInt(clusteringKeyValue.trim().substring(8));

			values = new Date(year1 - 1900, month1 - 1, day1);
		}

		columnNameValue.put(getClusteringKey(tableName), values);
		checkType = checkType(tableName, columnNameValue);
		checkMinMax = checkMinMax(tableName, columnNameValue);
		int indexOfTable = getIndexOfTable(tableName);
		Hashtable<String, Object> oldRowValues = null;
		if (checkMinMax && checkType && indexOfTable >= 0) {
			int updatePageIndex = getIndexOfUpdatePage(tableName, columnNameValue);
			Page p = deserializePage(tableName, updatePageIndex);
			for (int i = 0; i < p.page.size(); i++) {
				if (compareRowValues(tableName, columnNameValue, p.page.get(i)) == 0) {
					oldRowValues = p.page.get(i);
					p.page.set(i, columnNameValue);

					i += p.page.size();
					break;
				}
			}
			serializePage(p);

			// update index
			for (int j = 0; j < tables.get(indexOfTable).indices.size(); j++) {
				Index x = deserializeIndex(tables.get(indexOfTable).indices.get(j));
				int[] MDIndex = new int[x.columnNames.length];
				String BucketBath = null;
				for (int i = 0; i < x.columnNames.length; i++) {
					String[] TypeMinMax = getTypeMinMax(x.tableName, x.columnNames[i]);
					String[] rangeArr = generateRange(TypeMinMax[0], TypeMinMax[1], TypeMinMax[2], 10);
					int index = getIndexFromRanges(oldRowValues.get(x.columnNames[i]).toString(), TypeMinMax[0],
							rangeArr);
					if (index < 0)
						throw new DBAppException();
					MDIndex[i] = index;
				}
				Object[] O = x.gridIndexArray;
				for (int i = 0; i < MDIndex.length; i++) {
					if (i == MDIndex.length - 1) {
						BucketBath = (String) O[MDIndex[i]];
					} else {
						O = (Object[]) O[MDIndex[i]];
					}
				}
				Bucket b = deserializeBucket(BucketBath);
				boolean updated = false;
				;
				for (int k = 0; k < b.bucket.size(); k++) {
					if (compareRowValues(tableName, oldRowValues,
							(Hashtable<String, Object>) (b.bucket.get(k).get(0))) == 0) {
						b.bucket.get(k).set(0, columnNameValue);
						b.bucket.get(k).set(1, updatePageIndex);
						updated = true;
					}
				}
				if (!updated) {
					for (int i = 0; i < b.overFlowBuckets.size(); i++) {
						Bucket t = deserializeBucket(b.overFlowBuckets.get(i));
						for (int j1 = 0; j1 < t.bucket.size(); j1++) {
							if (compareRowValues(tableName, oldRowValues,
									(Hashtable<String, Object>) (t.bucket.get(j1).get(0))) == 0) {
								t.bucket.get(j1).set(0, columnNameValue);
								t.bucket.get(j1).set(1, updatePageIndex);
								updated = true;
							}
						}
						serializeBucket(t);
					}
				}

				serializeBucket(b);
				serializeIndex(x);
			}
		}

	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub

		boolean checkType = true;
		boolean checkMinMax = true;

		int indexOfTable = getIndexOfTable(tableName);
		if (checkMinMax && checkType && indexOfTable >= 0) {
			for (int i = 0; i < tables.get(indexOfTable).pages.size(); i++) {
				Page p = deserializePage(tableName, i);
				for (int j = 0; j < p.page.size(); j++) {
					boolean flag = true;
					for (int k = 0; k < columnNameValue.size(); k++) {
						String key = (columnNameValue.entrySet().toArray())[k].toString().split("=")[0].toString();
						Object a = columnNameValue.get(key);
						Object b = p.page.get(j).get(key);
						int compareVal = compareTwoValues(a, b);
						if (compareVal != 0) {
							flag = false;
							k += columnNameValue.size() + 1;

						}

					}
					if (flag) {
						System.out.println("deleting");
						// delete row from indicies
						for (int j1 = 0; j1 < tables.get(indexOfTable).indices.size(); j1++) {
							Index x = deserializeIndex(tables.get(indexOfTable).indices.get(j1));
							int[] MDIndex = new int[x.columnNames.length];
							String bucketPath = null;
							for (int i1 = 0; i1 < x.columnNames.length; i1++) {
								String[] TypeMinMax = getTypeMinMax(x.tableName, x.columnNames[i1]);
								String[] rangeArr = generateRange(TypeMinMax[0], TypeMinMax[1], TypeMinMax[2], 10);
								int index = getIndexFromRanges(p.page.get(j).get(x.columnNames[i1]).toString(),
										TypeMinMax[0], rangeArr);
								if (index < 0)
									throw new DBAppException();
								MDIndex[i1] = index;
							}
							Object[] O = x.gridIndexArray;
							for (int i1 = 0; i1 < MDIndex.length; i1++) {
								if (i1 == MDIndex.length - 1) {
									bucketPath = (String) O[MDIndex[i1]];
								} else {
									O = (Object[]) O[MDIndex[i1]];
								}
							}
							Bucket b = deserializeBucket(bucketPath);
							for (int k = 0; k < b.bucket.size(); k++) {
								if (compareRowValues(tableName, p.page.get(j),
										(Hashtable<String, Object>) (b.bucket.get(k).get(0))) == 0) {
									b.bucket.remove(k);

								}
							}
							for (int i1 = 0; i1 < b.overFlowBuckets.size(); i1++) {
								Bucket t = deserializeBucket(b.overFlowBuckets.get(i1));
								for (int k = 0; k < t.bucket.size(); k++) {
									if (compareRowValues(tableName, p.page.get(j),
											(Hashtable<String, Object>) (t.bucket.get(k).get(0))) == 0) {
										t.bucket.remove(k);

									}
								}
								serializeBucket(t);
							}
							serializeBucket(b);
							serializeIndex(x);
						}
						//

						// delete row from page
						p.page.remove(j);

						if (p.page.size() == 0) {
							p.deletePageFile();
							tables.get(indexOfTable).pages.set(i, null);

						}
					}

				}

				serializePage(p);
			}

		}
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub

		ArrayList<String> rows = new ArrayList<String>();
		Iterator<String> iterator = null;
		String tableName = sqlTerms[0]._strTableName;
		int tableIndex = getIndexOfTable(tableName);
		if (tableIndex == -1)
			throw new DBAppException("table does not exit");
		for (int i = 0; i < sqlTerms.length; i++) {
			checkColExists(tableName, sqlTerms[i]._strColumnName);
		}
		boolean hasIndex = false;
		// implement hasIndex logic
		boolean allAnd = true;
		for (int i = 0; i < arrayOperators.length; i++) {
			if (!(arrayOperators[i].equals("AND")) && !(arrayOperators[i].equals("and"))
					&& !(arrayOperators[i].equals("And"))) {
				allAnd = false;
			}
		}
		int indexOfIndices = -1;
		int matchingColumns = 0;
		int currentMatchingCol = 0;
		if (allAnd) {
			for (int i = 0; i < tables.get(tableIndex).indices.size(); i++) {
				Index x = deserializeIndex(tables.get(tableIndex).indices.get(i));
				currentMatchingCol = 0;
				for (int j = 0; j < sqlTerms.length; j++) {
					for (int k = 0; k < x.columnNames.length; k++) {

						if ((sqlTerms[j]._strColumnName).equals(x.columnNames[k])) {
							currentMatchingCol++;
							k+=x.columnNames.length+555;
						}
					}

				}
				if (currentMatchingCol <= sqlTerms.length && x.columnNames.length <= currentMatchingCol
						&& currentMatchingCol > matchingColumns) {
					matchingColumns = currentMatchingCol;
					indexOfIndices = i;
					hasIndex = true;
				}
				serializeIndex(x);
			}
		} else {
			int previousMatchingCol = 0;
			for (int i = 0; i < tables.get(tableIndex).indices.size(); i++) {
				Index x = deserializeIndex(tables.get(tableIndex).indices.get(i));
				currentMatchingCol = 0;
				for (int j = 0; j < sqlTerms.length; j++) {
					for (int k = 0; k < x.columnNames.length; k++) {
						if ((sqlTerms[j]._strColumnName).equals(x.columnNames[k])) {
							currentMatchingCol++;
							k+=x.columnNames.length+555;
						}
					}
				}
				if (currentMatchingCol <= sqlTerms.length && x.columnNames.length <= currentMatchingCol
						&& currentMatchingCol > previousMatchingCol) {
					previousMatchingCol = currentMatchingCol;
					indexOfIndices = i;
					hasIndex = true;
				}
				serializeIndex(x);
			}

		}

		//
		if (!hasIndex) {
			// if there is no index (linear scan)
			for (int i = 0; i < tables.get(tableIndex).pages.size(); i++) {
				Page p = deserializePage(tableName, i);
				for (int j = 0; j < p.page.size(); j++) {
					boolean matchQuery = doesMatchQuery(sqlTerms, arrayOperators, p.page.get(j));
					if (matchQuery) {
						// add to iterator
						rows.add(p.page.get(j).toString());
					}
				}
				serializePage(p);
			}

		} else {
			// if there is an index
			Index x = deserializeIndex(tables.get(tableIndex).indices.get(indexOfIndices));
			Hashtable<String, Object> targetRow = new Hashtable<String, Object>();
			for (int i = 0; i < sqlTerms.length; i++) {
				targetRow.put(sqlTerms[i]._strColumnName, sqlTerms[i]._objValue);
			}
			int[] MDIndex = new int[x.columnNames.length];
			String bucketPath = null;
			int index = 0;
			for (int i = 0; i < x.columnNames.length; i++) {
				String[] TypeMinMax = getTypeMinMax(x.tableName, x.columnNames[i]);
				String[] rangeArr = generateRange(TypeMinMax[0], TypeMinMax[1], TypeMinMax[2], 10);
				if(targetRow.get(x.columnNames[i])!=null)
					index = getIndexFromRanges(targetRow.get(x.columnNames[i]).toString(), TypeMinMax[0], rangeArr);
				if (index < 0)
					throw new DBAppException();
				MDIndex[i] = index;
			}
			Object[] O = x.gridIndexArray;
			for (int i = 0; i < MDIndex.length; i++) {
				if (i == MDIndex.length - 1) {
					bucketPath = (String) O[MDIndex[i]];
				} else {
					O = (Object[]) O[MDIndex[i]];
				}
			}
			Bucket b = deserializeBucket(bucketPath);
			for (int i = 0; i < b.bucket.size(); i++) {
				boolean matchQuery = doesMatchQuery(sqlTerms, arrayOperators,
						(Hashtable<String, Object>) b.bucket.get(i).get(0));
				if (matchQuery) {
					// add to iterator
					rows.add(((Hashtable<String, Object>) (b.bucket.get(i).get(0))).toString());
				}
			}
			for (int i = 0; i < b.overFlowBuckets.size(); i++) {
				Bucket t = deserializeBucket(b.overFlowBuckets.get(i));
				for (int j = 0; j < t.bucket.size(); j++) {
					boolean matchQuery = doesMatchQuery(sqlTerms, arrayOperators,
							(Hashtable<String, Object>) t.bucket.get(j).get(0));
					if (matchQuery) {
						// add to iterator
						rows.add(((Hashtable<String, Object>) (t.bucket.get(j).get(0))).toString());
					}
				}
				serializeBucket(t);
			}
			serializeBucket(b);
			serializeIndex(x);
		}
		iterator = rows.iterator();
		return iterator;
	}

	public boolean doesMatchQuery(SQLTerm[] sqlTerms, String[] arrayOperators, Hashtable<String, Object> row) {
		boolean doesMatch = false;
		Vector<Boolean> termResults = new Vector<Boolean>();
		if(sqlTerms.length==0)
			return true;
		for (int i = 0; i < sqlTerms.length; i++) {
			doesMatch = false;
			String[] s = getTypeMinMax(sqlTerms[0]._strTableName, sqlTerms[i]._strColumnName);
			String colType = s[0];
			if (colType.equals("java.lang.String")) {
				String queryValue = (String) sqlTerms[i]._objValue;
				String rowValue = (String) row.get(sqlTerms[i]._strColumnName);
				int compareVal = rowValue.compareTo(queryValue);
				switch (sqlTerms[i]._strOperator) {
				// >, >=, <, <=, != or =

				case ">":
					if (compareVal > 0)
						doesMatch = true;
					break;
				case ">=":
					if (compareVal >= 0)
						doesMatch = true;
					break;
				case "<":
					if (compareVal < 0)
						doesMatch = true;
					break;
				case "<=":
					if (compareVal <= 0)
						doesMatch = true;
					break;
				case "!=":
					if (compareVal != 0)
						doesMatch = true;
					break;
				case "=":
					if (compareVal == 0)
						doesMatch = true;
					break;
				}

			} else if (colType.equals("java.lang.Integer")) {
				int queryValue = Integer.parseInt((String) sqlTerms[i]._objValue);
				int rowValue = Integer.parseInt((String) row.get((String) sqlTerms[i]._objValue));
				switch (sqlTerms[i]._strOperator) {
				// >, >=, <, <=, != or =

				case ">":
					if (rowValue > queryValue)
						doesMatch = true;
					break;
				case ">=":
					if (rowValue >= queryValue)
						doesMatch = true;
					break;
				case "<":
					if (rowValue < queryValue)
						doesMatch = true;
					break;
				case "<=":
					if (rowValue <= queryValue)
						doesMatch = true;
					break;
				case "!=":
					if (rowValue != queryValue)
						doesMatch = true;
					break;
				case "=":
					if (rowValue == queryValue)
						doesMatch = true;
					break;
				}

			}

			else if (colType.equals("java.lang.Double")) {
				Double queryValue = (Double) sqlTerms[i]._objValue;
				Double rowValue = (Double) row.get(sqlTerms[i]._strColumnName);
				switch (sqlTerms[i]._strOperator) {
				// >, >=, <, <=, != or =

				case ">":
					if (rowValue.doubleValue() > queryValue.doubleValue())
						doesMatch = true;
					break;
				case ">=":
					if (rowValue.doubleValue() >= queryValue.doubleValue())
						doesMatch = true;
					break;
				case "<":
					if (rowValue.doubleValue() < queryValue.doubleValue())
						doesMatch = true;
					break;
				case "<=":
					if (rowValue.doubleValue() <= queryValue.doubleValue())
						doesMatch = true;
					break;
				case "!=":
					if (rowValue.doubleValue() != queryValue.doubleValue())
						doesMatch = true;
					break;
				case "=":
					if (rowValue.doubleValue() == queryValue.doubleValue())
						doesMatch = true;
					break;
				}
			} else if (colType.equals("java.util.Date")) {

				int yearQuery = Integer.parseInt(((String) (sqlTerms[i]._objValue)).trim().substring(0, 4));
				int monthQuery = Integer.parseInt(((String) (sqlTerms[i]._objValue)).trim().substring(5, 7));
				int dayQuery = Integer.parseInt(((String) (sqlTerms[i]._objValue)).trim().substring(8));
				Date queryValue = new Date(yearQuery - 1900, monthQuery - 1, dayQuery);

				int yearRow = Integer.parseInt(((String) row.get(sqlTerms[i]._strColumnName)).trim().substring(0, 4));
				int monthRow = Integer.parseInt(((String) row.get(sqlTerms[i]._strColumnName)).trim().substring(5, 7));
				int dayRow = Integer.parseInt(((String) row.get(sqlTerms[i]._strColumnName)).trim().substring(8));
				Date rowValue = new Date(yearRow - 1900, monthRow - 1, dayRow);

				switch (sqlTerms[i]._strOperator) {
				// >, >=, <, <=, != or =

				case ">":
					if (rowValue.after(queryValue))
						doesMatch = true;
					break;
				case ">=":
					if (rowValue.after(queryValue) || rowValue.equals(queryValue))
						doesMatch = true;
					break;
				case "<":
					if (rowValue.before(queryValue))
						doesMatch = true;
					break;
				case "<=":
					if (rowValue.before(queryValue) || rowValue.equals(queryValue))
						doesMatch = true;
					break;
				case "!=":
					if (!rowValue.equals(queryValue))
						doesMatch = true;
					break;
				case "=":
					if (rowValue.equals(queryValue))
						doesMatch = true;
					break;
				}

			}
			termResults.add(doesMatch);

		}
		for (int i = 0; i < arrayOperators.length; i++) {
			switch (arrayOperators[i]) {
			case "OR":
			case "Or":
			case "or":
				boolean res = termResults.get(i) || termResults.get(i + 1);
				termResults.remove(0);
				termResults.set(0, res);
				break;
			case "AND":
			case "And":
			case "and":
				boolean res1 = termResults.get(i) && termResults.get(i + 1);
				termResults.remove(0);
				termResults.set(0, res1);
				break;
			case "XOR":
			case "Xor":
			case "xor":
				boolean res2 = !(termResults.get(i)) && termResults.get(i + 1);
				termResults.remove(0);
				termResults.set(0, res2);
				break;

			}
		}

		return termResults.get(0);
	}

	public boolean checkColExists(String tableName, String colName) throws DBAppException {
		boolean exists = false;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String row;

			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					if (data[1].equals(colName)) {
						exists = true;
					}
				}

			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (!exists)
			throw new DBAppException("column does not exist");
		return exists;
	}

	public boolean checkType(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// check if the values of inserted row is of the same type of the table
		boolean valid = true;
		int noOfOccur = 0;
		String row;

		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			BufferedReader csvReader1 = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			ArrayList<String> columnNames = new ArrayList<String>();
			;
			while ((row = csvReader1.readLine()) != null) {

				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					columnNames.add(data[1]);
				}
			}
			for (int i = 0; i < colNameValue.size(); i++) {
				boolean flag = false;
				String colName = (colNameValue.entrySet().toArray())[i].toString().split("=")[0].toString();
				for (int j = 0; j < columnNames.size(); j++) {
					if (colName.equals(columnNames.get(j))) {
						flag = true;
						j += columnNames.size() + 1;
					}
				}
				if (!flag) {
					throw new DBAppException("column does not exist");
				}

			}

			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					noOfOccur++;
					for (int i = 0; i < colNameValue.size(); i++) {
						String colName = (colNameValue.entrySet().toArray())[i].toString().split("=")[0].toString();
						if (data[1].equals(colName)) {
							if (colNameValue.get(colName) instanceof Integer) {
								if (data[2].equals("java.lang.Integer")) {

								} else {
									valid = false;
									throw new DBAppException("incompaitable data type (should be int)");
								}
							} else if (colNameValue.get(colName) instanceof String) {
								if (data[2].equals("java.lang.String")) {

								} else {
									valid = false;
									throw new DBAppException("incompaitable data type (should be String)");
								}
							} else if (colNameValue.get(colName) instanceof Double) {
								if (data[2].equals("java.lang.Double")) {

								} else {
									valid = false;
									throw new DBAppException("incompaitable data type (should be double)");
								}
							} else if (colNameValue.get(colName) instanceof Date) {
								if (data[2].equals("java.util.Date") || data[2].equals("java.lang.Date")) {

								} else {
									valid = false;
									throw new DBAppException("incompaitable data type (should be date)");
								}
							} else {
								valid = false;
								throw new DBAppException("data type not supported");
							}

						}
					}
				}

			}

			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (noOfOccur == 0) {
			valid = false;
			throw new DBAppException("table doesn't exist");
		}
		return valid;
	}

	public boolean checkMinMax(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// check if the entered values is in the range
		boolean valid = true;
		int noOfOccur = 0;
		String row;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					noOfOccur++;
					for (int i = 0; i < colNameValue.size(); i++) {
						String colName = (colNameValue.entrySet().toArray())[i].toString().split("=")[0].toString();
						if (data[1].equals(colName)) {
							if (colNameValue.get(colName) instanceof Integer) {
								if ((Integer) (colNameValue.get(colName)) >= (Integer.parseInt(data[5]))
										&& (Integer) (colNameValue.get(colName)) <= (Integer.parseInt(data[6]))) {

								} else {
									valid = false;
									throw new DBAppException("int value not in range");
								}
							} else if (colNameValue.get(colName) instanceof String) {
								int min = ((String) colNameValue.get(colName)).compareTo(data[5]);
								int max = ((String) colNameValue.get(colName)).compareTo(data[6]);
								if (min >= 0 && max <= 0) {

								} else {
									valid = false;
									throw new DBAppException("string value not in range");
								}
							} else if (colNameValue.get(colName) instanceof Double) {
								if ((Double) (colNameValue.get(colName)) >= (Double.parseDouble(data[5]))
										&& (Double) (colNameValue.get(colName)) <= (Double.parseDouble(data[6]))) {

								} else {
									valid = false;
									throw new DBAppException("double value not in range");
								}
							} else if (colNameValue.get(colName) instanceof Date) {

								Date d = (Date) (colNameValue.get(colName));
								int year1 = Integer.parseInt(data[5].trim().substring(0, 4));
								int month1 = Integer.parseInt(data[5].trim().substring(5, 7));
								int day1 = Integer.parseInt(data[5].trim().substring(8));

								Date min = new Date(year1 - 1900, month1 - 1, day1);
								int year2 = Integer.parseInt(data[6].trim().substring(0, 4));
								int month2 = Integer.parseInt(data[6].trim().substring(5, 7));
								int day2 = Integer.parseInt(data[6].trim().substring(8));

								Date max = new Date(year2 - 1900, month2 - 1, day2);

								if (d.after(min) && d.before(max)) {

								}

								else {
									valid = false;
									System.out.println(colNameValue.get("first_name"));
									System.out.println(colNameValue.get("last_name"));

									System.out.println("inserted date: " + d);
									System.out.println("min: " + min);
									System.out.println("max: " + max);

									throw new DBAppException("date value not in range");
								}
							} else {
								throw new DBAppException("data type not supported");
							}

						}
					}
				}

			}
			if (noOfOccur == 0) {
				valid = false;
				throw new DBAppException("table doesn't exist");
			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return valid;
	}

	public String getClusteringKey(String tableName) {

		String clusteringKey = "";
		String row;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName) && data[3].equals("true")) {
					clusteringKey = data[1];
				}

			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return clusteringKey;

	}

	public String getClusteringKeyType(String tableName) {

		String clusteringKey = "";
		String row;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName) && data[3].equals("true")) {
					clusteringKey = data[2];
				}

			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return clusteringKey;

	}

	public int getIndexOfTargetPage(String tableName, Hashtable<String, Object> colNameValue) {
		// binary searching pages

		int indexOfTable = getIndexOfTable(tableName);
		int left = 0;
		int right = tables.get(indexOfTable).pages.size() - 1;
		int targetPageIndex = 0;
		int middle = 0;
		Page p = null;
		while (left <= right) {

			if (tables.get(indexOfTable).pages.get(middle) != null) {
				middle = left + (right - left) / 2;
				p = deserializePage(tableName, middle);
				if (p.page.isEmpty()) {
					return middle;
				}
				Hashtable<String, Object> maxRow = p.page.get((p.page.size()) - 1);
				Hashtable<String, Object> minRow = (p.page).get(0);
				int comparWithMax = compareRowValues(tableName, colNameValue, maxRow);
				int comparWithMin = compareRowValues(tableName, colNameValue, minRow);
				if (comparWithMin >= 0 && comparWithMax <= 0) {
					targetPageIndex = middle;
					return targetPageIndex;
				} else if (comparWithMin < 0) {
					if (middle == 0) {
						return middle;
					} else {
						Page p1 = deserializePage(tableName, middle - 1);
						Hashtable<String, Object> maxRowBefore = p1.page.get((p1.page.size()) - 1);
						int comparWithMaxBefore = compareRowValues(tableName, colNameValue, maxRowBefore);
						if (comparWithMaxBefore >= 0) {
							return middle;
						} else {
							right = middle - 1;
						}
						serializePage(p1);
					}

				} else if (comparWithMax > 0) {
					if (middle == tables.get(indexOfTable).pages.size() - 1) {
						return middle;
					} else {
						Page p2 = deserializePage(tableName, middle + 1);
						if (!p2.page.isEmpty()) {
							Hashtable<String, Object> minRowAfter = p2.page.get(0);
							int comparWithMinAfter = compareRowValues(tableName, colNameValue, minRowAfter);
							if (comparWithMin < 0) {
								return middle + 1;
							} else {
								left = middle + 1;
							}
						} else {
							return middle + 1;
						}
						serializePage(p2);
					}

				}
			} else {
				middle++;
			}
			serializePage(p);

		}
		return targetPageIndex;

	}

	public int getIndexOfUpdatePage(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// binary searching pages

		int indexOfTable = getIndexOfTable(tableName);
		int left = 0;
		int right = tables.get(indexOfTable).pages.size() - 1;
		int targetPageIndex = -1;
		int middle = 0;
		Page p = null;
		while (left <= right) {

			if (tables.get(indexOfTable).pages.get(middle) != null) {
				middle = left + (right - left) / 2;

				p = deserializePage(tableName, middle);

				Hashtable<String, Object> maxRow = p.page.get((p.page.size()) - 1);
				Hashtable<String, Object> minRow = (p.page).get(0);
				int comparWithMax = compareRowValues(tableName, colNameValue, maxRow);
				int comparWithMin = compareRowValues(tableName, colNameValue, minRow);
				if (comparWithMin >= 0 && comparWithMax <= 0) {
					targetPageIndex = middle;
					return targetPageIndex;
				} else if (comparWithMin < 0) {

					right = middle - 1;

				} else if (comparWithMax > 0) {
					if (middle == tables.get(indexOfTable).pages.size() - 1) {
						return middle;
					} else {
						left = middle + 1;
					}
				}
			} else {
				middle++;
			}
			serializePage(p);

		}
		if (targetPageIndex < 0) {
			throw new DBAppException("no column with this key exists in the table");
		}
		return targetPageIndex;

	}

	public int getIndexOfTable(String tableName) {
		// get index of the table that we need from vector of tables
		int indexOfTable = -1;
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).name.equals(tableName)) {
				indexOfTable = i;
				i += tables.size();
			}
		}
		return indexOfTable;
	}

	public int compareRowValues(String tableName, Hashtable<String, Object> colNameValue1,
			Hashtable<String, Object> colNameValue2) {
		// compare two rows based on the clustering key of the table
		// return 0 if = , -1 if < , 1 if >
		String clusteringKey = getClusteringKey(tableName);
		String clusteringKeyType = getClusteringKeyType(tableName);
		int answer = 0;

		if (clusteringKeyType.equals("java.lang.Integer")) {
			if (((int) (colNameValue1.get(clusteringKey))) == ((int) (colNameValue2.get(clusteringKey))))
				answer = 0;

			else if (((int) (colNameValue1.get(clusteringKey))) < ((int) (colNameValue2.get(clusteringKey))))
				answer = -1;

			else if (((int) (colNameValue1.get(clusteringKey))) > ((int) (colNameValue2.get(clusteringKey))))
				answer = 1;

		} else if (clusteringKeyType.equals("java.lang.Double")) {
			if (((double) (colNameValue1.get(clusteringKey))) == ((double) (colNameValue2.get(clusteringKey))))
				answer = 0;

			else if (((double) (colNameValue1.get(clusteringKey))) < ((double) (colNameValue2.get(clusteringKey))))
				answer = -1;

			else if (((double) (colNameValue1.get(clusteringKey))) > ((double) (colNameValue2.get(clusteringKey))))
				answer = 1;

		}

		else if (clusteringKeyType.equals("java.lang.String")) {
			answer = ((String) (colNameValue1.get(clusteringKey)))
					.compareTo(((String) (colNameValue2.get(clusteringKey))));

		} else if (clusteringKeyType.equals("java.lang.Date") || clusteringKeyType.equals("java.util.Date")) {

			Date d1 = (Date) (colNameValue1.get(clusteringKey));
			Date d2 = (Date) (colNameValue2.get(clusteringKey));

			if (d1.before(d2)) {
				answer = -1;
			} else if (d1.equals(d2)) {
				answer = 0;
			} else if (d1.after(d2)) {
				answer = 1;
			}

		}
		return answer;
	}

	public int compareTwoValues(Object a, Object b) {
		int n = 0;
		if ((a instanceof Integer && b instanceof Integer)) {
			if ((int) a == (int) b)
				n = 0;
			else if ((int) a > (int) b)
				n = 1;
			else if ((int) a < (int) b)
				n = -1;

		} else if ((a instanceof Double && b instanceof Double)) {
			if ((Double) a == (Double) b)
				n = 0;
			else if ((Double) a > (Double) b)
				n = 1;
			else if ((Double) a < (Double) b)
				n = -1;
		} else if ((a instanceof String && b instanceof String)) {
			n = ((String) a).compareTo((String) b);
		} else if ((a instanceof Date && b instanceof Date)) {
			if (((Date) (a)).before((Date) (a))) {
				n = -1;
			} else if (((Date) (a)).after((Date) (a))) {
				n = 1;
			} else {
				n = 0;
			}
		}
		return n;
	}

	public void insertintoSortedPage(String tableName, Page p, Hashtable<String, Object> colNameValue) {
		boolean shift = true;
		if (p.page.isEmpty()) {
			p.page.add(colNameValue);
		} else if (p.page.size() == 1) {
			int compareVal = compareRowValues(tableName, colNameValue, p.page.get(0));
			if (compareVal < 0) {
				p.page.add(p.page.get(0));
				p.page.set(0, colNameValue);
			} else {
				p.page.add(colNameValue);
			}
		} else if (p.page.size() == 2) {
			int compareVal = compareRowValues(tableName, colNameValue, p.page.get(0));
			if (compareVal < 0) {
				p.page.add(p.page.get(1));
				p.page.set(1, p.page.get(0));
				p.page.set(0, colNameValue);
			} else {
				int compareVal2 = compareRowValues(tableName, colNameValue, p.page.get(1));
				if (compareVal2 < 0) {
					p.page.add(p.page.get(1));
					p.page.set(1, colNameValue);
				} else {
					p.page.add(colNameValue);
				}
			}
		} else if (p.page.size() > 2) {
			int targetIndex = 1;
			for (int i = 0; i < p.page.size(); i++) {
				int compareVal1 = compareRowValues(tableName, colNameValue, p.page.get(i));
				if (i == p.page.size() - 1) {

					if (compareVal1 < 0) {
						p.page.add(p.page.get(i));
						p.page.set(i, colNameValue);
						i += p.page.size() + 500;
						shift = false;
					} else {
						p.page.add(colNameValue);
						i += p.page.size() + 500;
						shift = false;
					}
				} else {
					if (compareVal1 < 0) {

						targetIndex = i;

						i += p.page.size() + 22;

					}

				}
			}
			if (shift) {
				Hashtable<String, Object> lastRow = (Hashtable<String, Object>) p.page.get(p.page.size() - 1).clone();
				p.page.add(lastRow);

				for (int j = p.page.size() - 2; j > targetIndex; j--) {

					p.page.set(j, p.page.get(j - 1));
				}
				p.page.set(targetIndex, colNameValue);
			}
		}
	}

	public void serializePage(Page p) {
		try {
			String fileName = "src/main/resources/data/" + p.tableName + "." + p.pageId + ".ser";
			FileOutputStream fileOut = new FileOutputStream(fileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("serialized unsuccessfully");
		}
	}

	public Page deserializePage(String tableName, int pageId) {
		Page p = null;
		try {

			String fileName = "src/main/resources/data/" + tableName + "." + pageId + ".ser";
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("deserialized unsuccessfully");

		} catch (ClassNotFoundException c) {
			System.out.println("deserialized unsuccessfully");
			c.printStackTrace();

		}

		return p;
	}

	public void serializeBucket(Bucket b) {
		try {
			String fileName = "src/main/resources/data/" + b.indexName + b.bucketNumber + ".Bucket" + ".ser";
			FileOutputStream fileOut = new FileOutputStream(fileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(b);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("serialized unsuccessfully");
		}
	}

	public Bucket deserializeBucket(String BucketPath) {
		Bucket B = null;
		try {

			String fileName = "src/main/resources/data/" + BucketPath + ".Bucket" + ".ser";
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			B = (Bucket) in.readObject();
			in.close();
			fileIn.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("deserialized unsuccessfully");

		} catch (ClassNotFoundException c) {
			System.out.println("deserialized unsuccessfully");
			c.printStackTrace();

		}

		return B;
	}

	public void serializeIndex(Index x) {
		try {
			String fileName = "src/main/resources/data/" + x.Name + ".Index" + ".ser";
			FileOutputStream fileOut = new FileOutputStream(fileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(x);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("serialized unsuccessfully");
		}
	}

	public Index deserializeIndex(String Name) {
		Index I = null;
		try {

			String fileName = "src/main/resources/data/" + Name + ".Index" + ".ser";
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			I = (Index) in.readObject();
			in.close();
			fileIn.close();

		} catch (IOException i) {
			i.printStackTrace();
			System.out.println("deserialized unsuccessfully");

		} catch (ClassNotFoundException c) {
			System.out.println("deserialized unsuccessfully");
			c.printStackTrace();

		}

		return I;
	}

	public Object[] createNDimArrayHelper(Object[] gridIndexArray, int totalNumOfDimensions, int currentLevel) {
		if (totalNumOfDimensions == 1)
			return gridIndexArray;

		currentLevel++;

		if (currentLevel == totalNumOfDimensions) {
			return null;
		}
		if (currentLevel < totalNumOfDimensions) {

			for (int i = 0; i < gridIndexArray.length; i++) {
				gridIndexArray[i] = new Object[gridIndexArray.length];
				createNDimArrayHelper((Object[]) gridIndexArray[i], totalNumOfDimensions, currentLevel);
			}

		}

		return gridIndexArray;
	}

	public Object[] createNDimArray(Object[] gridIndexArray, int totalNumOfDimensions) {
		return createNDimArrayHelper(gridIndexArray, totalNumOfDimensions, 0);
	}

	public void insertIntoIndex(Index x, Hashtable<String, Object> row, int pageNum) throws DBAppException {
		// implement later
		int[] MDIndex = new int[x.columnNames.length];
		String bucketPath = null;
		int index;
		for (int i = 0; i < x.columnNames.length; i++) {
			String[] TypeMinMax = getTypeMinMax(x.tableName, x.columnNames[i]);
			String[] rangeArr = generateRange(TypeMinMax[0], TypeMinMax[1], TypeMinMax[2], 10);
			if (row.get(x.columnNames[i]) instanceof Date) {
				Date d = (Date) row.get(x.columnNames[i]);
				String val = (d.getYear() + 1900) + "-" + (d.getMonth() + 1) + "-" + d.getDay();
				index = getIndexFromRanges(val, TypeMinMax[0], rangeArr);
			} else {
				index = getIndexFromRanges(row.get(x.columnNames[i]).toString(), TypeMinMax[0], rangeArr);
			}
			if (index < 0)
				throw new DBAppException();
			MDIndex[i] = index;
		}
		Object[] O = x.gridIndexArray;
		for (int i = 0; i < MDIndex.length; i++) {
			if (i == MDIndex.length - 1) {
				bucketPath = (String) O[MDIndex[i]];
			} else {
				O = (Object[]) O[MDIndex[i]];
			}
		}

		if (bucketPath == null) {
			// create a new bucket and insert it in this reference
			Bucket b = new Bucket(x.Name, MDIndex);
			bucketPath = b.Name;
			Vector v = new Vector<Object>();
			v.add(row);
			v.add(pageNum);
			addToBucket(b, v);
			serializeBucket(b);
		} else {
			// deserialize and insert into this reference
//			String s = "";
//			for (int i = 0; i < MDIndex.length; i++) {
//				s += MDIndex[i];
//
//			}

			Bucket b = deserializeBucket(bucketPath);
			Vector v = new Vector<Object>();
			v.add(row);
			v.add(pageNum);
			addToBucket(b, v);
			serializeBucket(b);
		}

	}

	public String[] getTypeMinMax(String tableName, String colName) {
		// [type,min,max]
		String s[] = new String[3];
		;
		//
		String row;
		try {
			BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName) && data[1].equals(colName)) {
					s[0] = data[2];
					s[1] = data[5];
					s[2] = data[6];
				}

			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return s;
	}

	public String[] generateRange(String type, String min, String max, int numOfDivisions) {
		String s[] = new String[numOfDivisions];
		;
		// left inclusive right exclusive
		if (type.equals("java.lang.Integer")) {
			int maxInt = Integer.parseInt(max);
			int minInt = Integer.parseInt(min);
			int intIncrement = (maxInt - minInt) / numOfDivisions;
			int leftRange = minInt;
			for (int i = 0; i < s.length; i++) {
				String tmp = leftRange + ":" + (leftRange + intIncrement);
				s[i] = tmp;
				if (i == s.length - 1) {
					tmp = leftRange + ":" + (maxInt);
					s[i] = tmp;
				}
				leftRange += intIncrement;

			}

		} else if (type.equals("java.lang.Double")) {
			Double maxDouble = Double.parseDouble(max);
			Double minDouble = Double.parseDouble(min);
			Double DoubleIncrement = (maxDouble - minDouble) / numOfDivisions;
			Double leftRange = minDouble;
			for (int i = 0; i < s.length; i++) {
				String tmp = leftRange + ":" + (leftRange + DoubleIncrement);
				s[i] = tmp;
				if (i == s.length - 1) {
					tmp = leftRange + ":" + (maxDouble);
					s[i] = tmp;
				}
				leftRange += DoubleIncrement;

			}

		} else if (type.equals("java.lang.String")) {
			Long minAsci = stringToAscii(min);
			Long maxAsci = stringToAscii(max);
			Long increment = (maxAsci - minAsci) / numOfDivisions;
			Long leftRange = minAsci;
			for (int i = 0; i < s.length; i++) {
				String tmp = leftRange + ":" + (leftRange + increment);
				s[i] = tmp;
				if (i == s.length - 1) {
					tmp = leftRange + ":" + (maxAsci);
					s[i] = tmp;
				}
				leftRange += increment;

			}

		} else if (type.equals("java.util.Date")) {
			int yearMin = Integer.parseInt(min.trim().substring(0, 4));
			int monthMin = Integer.parseInt(min.trim().substring(5, 7));
			int dayMin = Integer.parseInt(min.trim().substring(8));
			Date minDate = new Date(yearMin - 1900, monthMin - 1, dayMin);

			int yearMax = Integer.parseInt(max.trim().substring(0, 4));
			int monthMax = Integer.parseInt(max.trim().substring(5, 7));
			int dayMax = Integer.parseInt(max.trim().substring(8));
			Date maxDate = new Date(yearMax - 1900, monthMax - 1, dayMax);

			int yearIncrement = (maxDate.getYear() - minDate.getYear()) / numOfDivisions;
			//System.out.println(maxDate.getYear() - minDate.getYear());
			Date leftRange = minDate;
			//int temp=minDate.getDay();
			int temp1=maxDate.getDay();
			if(temp1==0)
				temp1=31;
			for (int i = 0; i < s.length; i++) {
				if (i == s.length - 1) {

					s[i] = (leftRange.getYear() + 1900) + "-" + (leftRange.getMonth() + 1) + "-" + leftRange.getDay()
							+ ":" + (maxDate.getYear() + 1900) + "-" + (maxDate.getMonth() + 1) + "-"
							+ (temp1 );
				} else {
					String tmp = (leftRange.getYear() + 1900) + "-" + (leftRange.getMonth() + 1) + "-"
							+ leftRange.getDay() + ":";
					leftRange.setYear(leftRange.getYear() + yearIncrement);
					tmp += (leftRange.getYear() + 1900) + "-" + (leftRange.getMonth() + 1) + "-" + leftRange.getDay();
					s[i] = tmp;
				}
			}

		}

		return s;
	}

	public int getIndexFromRanges(String value, String type, String[] rangeValuesArr) {
		int n = -1;
		String min = (rangeValuesArr[0].split(":"))[0];
		String max = (rangeValuesArr[rangeValuesArr.length - 1].split(":"))[1];
		for (int i = 0; i < rangeValuesArr.length; i++) {
			String[] currentRangeArr = rangeValuesArr[i].split(":");
			String currentMin = currentRangeArr[0];
			String currentMax = currentRangeArr[1];

			if (type.equals("java.lang.Integer")) {
				int val = Integer.parseInt(value);
				int minInt = Integer.parseInt(currentMin);
				int maxInt = Integer.parseInt(currentMax);
				if (i == rangeValuesArr.length - 1) {
					if (val >= minInt && val <= maxInt)
						return i;

					else if (val < minInt)
						return i - 1;
				} else {
					if (val >= minInt && val < maxInt)
						return i;

					else if (val < minInt)
						return i - 1;
				}
			} else if (type.equals("java.lang.Double")) {
				Double val = Double.parseDouble(value);
				Double minDouble = Double.parseDouble(currentMin);
				Double maxDouble = Double.parseDouble(currentMax);
				if (i == rangeValuesArr.length - 1) {
					if (val >= minDouble && val <= maxDouble)
						return i;

					else if (val < minDouble)
						return i - 1;
				} else {

					if (val >= minDouble && val < maxDouble)
						return i;

					else if (val < minDouble)
						return i - 1;
				}
			} else if (type.equals("java.util.Date")) {
				String[] minTmp = currentMin.split("-");
				int yearMin = Integer.parseInt(minTmp[0]);
				int monthMin = Integer.parseInt(minTmp[1]);
				int dayMin = Integer.parseInt(minTmp[2]);
				Date minDate = new Date(yearMin - 1900, monthMin - 1, dayMin);

				String[] maxTmp = currentMax.split("-");
				int yearMax = Integer.parseInt(maxTmp[0]);
				int monthMax = Integer.parseInt(maxTmp[1]);
				int dayMax = Integer.parseInt(maxTmp[2]);
				Date maxDate = new Date(yearMax - 1900, monthMax - 1, dayMax);

				String[] valTemp = value.split("-");
				int year1 = Integer.parseInt(valTemp[0]);
				int month1 = Integer.parseInt(valTemp[1]);
				int day1 = Integer.parseInt(valTemp[2]);
				Date dateValue = new Date(year1 - 1900, month1 - 1, day1);
				if (i == rangeValuesArr.length - 1) {
					if ((dateValue.after(minDate) || dateValue.equals(minDate))
							&& (dateValue.before(maxDate) || dateValue.equals(maxDate)))
						return i;
					else if (dateValue.before(minDate))
						return i - 1;
				} else {
					if ((dateValue.after(minDate) || dateValue.equals(minDate)) && dateValue.before(maxDate))
						return i;
					else if (dateValue.before(minDate))
						return i - 1;
				}
			} else if (type.equals("java.lang.String")) {
				String minString = asciiToString(min);
				String maxString = asciiToString(max);
				if (value.length() < minString.length() || value.length() > maxString.length()) {
					return -1;
				}

				Long valueAscii = stringToAscii(value);
				Long minAscii = Long.parseLong(currentMin);
				Long maxAscii = Long.parseLong(currentMax);
				if (i == rangeValuesArr.length - 1) {
					if (valueAscii >= minAscii && valueAscii <= maxAscii) {
						return i;
					} else if (valueAscii < minAscii) {
						return i - 1;
					}
				} else {
					if (valueAscii >= minAscii && valueAscii < maxAscii) {
						return i;
					} else if (valueAscii < minAscii) {
						return i - 1;
					}
				}
			}
		}

		return n;

	}

	public static Long stringToAscii(String s) throws NumberFormatException {
		String res = "";
		for (int i = 0; i < s.length(); i++) {
			int temp = s.charAt(i);
			res += temp;
		}
		Long x = Long.parseLong(res);
		return x;
	}

	static String asciiToString(String str) {
		int len = str.length();
		int num = 0;
		String ans = "";
		for (int i = 0; i < len; i++) {

			// Append the current digit
			num = num * 10 + (str.charAt(i) - '0');

			// If num is within the required range
			if (num >= 32 && num <= 122) {

				// Convert num to char
				char ch = (char) num;
				ans += ch;

				// Reset num to 0
				num = 0;
			}
		}
		return ans;
	}

	public void addToBucket(Bucket b, Vector<Object> v) {
		boolean added = false;
		if (b.bucket.size() == b.maxEntries) {
			for (int i = 0; i < b.overFlowBuckets.size(); i++) {
				Bucket t = deserializeBucket(b.overFlowBuckets.get(i));
				if (t.bucket.size() < t.maxEntries) {
					t.bucket.add(v);
					added = true;
					i += b.overFlowBuckets.size() + 55;
				}
				serializeBucket(t);
			}
			if (!added) {
				Bucket overflow = new Bucket(b);
				overflow.bucket.add(v);
				serializeBucket(overflow);
			}

		} else {
			b.bucket.add(v);
		}
	}

	public static void main(String[] args) throws Exception {
		 
	}
}
