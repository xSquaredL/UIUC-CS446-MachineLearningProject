package com.cs446.ml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class HillaryExplorerApache {
	private static final String sqlitePath = "/Users/xin/Documents/Develop/CS446/HillaryEmailsAnalyst/data/database.sqlite";
	private static final String tokenizerModelPath = "/Users/xin/Documents/Develop/CS446/HillaryEmailsAnalyst/nlp/en-token.bin";
	private static final String stopWordPath = "/Users/xin/Documents/Develop/CS446/HillaryEmailsAnalyst/nlp/stopwords.txt";
	private static final String featurePath = "/Users/xin/Desktop/features_err.arff";
	private static final String dataPath = "/Users/xin/Desktop/data.txt";
	private static final String sql = "SELECT ExtractedBodyText,ExtractedSubject,MetadataTo,ExtractedDateSent,MetadataFrom FROM Emails;";

	private Set<String> stopWords;
	private Set<String> labelSet;
	private Tokenizer tokenizer;
	private HashMap<String,Integer> bodyDict;
	private HashMap<String,Integer> subjectDict;
	private List<String> bodyFeatures;
	private List<String> subjectFeatures;
	private int bodyMaxCount;
	private int subMaxCount;

	private Connection c;
	private Statement stmt;

	public HillaryExplorerApache() throws Exception{
		stopWords = new HashSet<String>();
		labelSet = new HashSet<String>();
		bodyDict = new HashMap<String,Integer>();
		subjectDict = new HashMap<String,Integer>();
		bodyFeatures = new ArrayList<String>();
		subjectFeatures = new ArrayList<String>();
		bodyMaxCount=0;
		subMaxCount=0;

		getStopWords();

		c = DriverManager.getConnection("jdbc:sqlite:"+sqlitePath);
		c.setAutoCommit(false);
		System.out.println("Opened database successfully");
		stmt = c.createStatement();

		File file = new File(tokenizerModelPath);
		TokenizerModel model = new TokenizerModel(file);
		tokenizer = new TokenizerME(model);
		getDictionary2();
	}

	private void closeConnection() throws Exception {
		stmt.close();
		c.close();
	}

	private void getStopWords() throws Exception {
		String sCurrentLine;
		FileReader fr=new FileReader(stopWordPath);
		BufferedReader br= new BufferedReader(fr);
		while ((sCurrentLine = br.readLine()) != null){
			stopWords.add(sCurrentLine);
		}
	}

	private void getDictionary() throws Exception {
		Set<String> bodySet = new HashSet<String>();
		Set<String> subjectSet = new HashSet<String>();

		ResultSet rs = stmt.executeQuery(sql );
		while ( rs.next() ) {
			String emailBody = rs.getString("ExtractedBodyText");
			String emailSubject = rs.getString("ExtractedSubject");
			String label = rs.getString("label");
			String tokens[] = tokenizer.tokenize(emailBody);
			labelSet.add(label);
			for (String w : tokens){
				if(!isSpecialChar(w)&&!stopWords.contains(w.toLowerCase())){
					w = w.replaceAll("[-+^,'\",?;!]","");
					bodySet.add(w);
				}
			}
			String subTokens[] = tokenizer.tokenize(emailSubject);
			for (String w : subTokens){
				if(!isSpecialChar(w)&&!stopWords.contains(w.toLowerCase())){
					w = w.replaceAll("[-+^,'\",?;!]","");
					subjectSet.add(w);
				}
			}
		}
		rs.close();

		int idx = 0;
		for(String w:bodySet){
			bodyDict.put(w, idx);
			idx++;
			bodyFeatures.add(w);
		}

		for(String w:subjectSet){
			subjectDict.put(w, idx);
			idx++;
			subjectFeatures.add(w);
		}
	}

	private void getDictionary2() throws Exception {
		Set<String> bodySet = new HashSet<String>();
		Set<String> subjectSet = new HashSet<String>();
		Set<String> receivers = new HashSet<String>();
		receivers.add("abedin, huma");
		receivers.add("abedinh@state.gov");
		receivers.add("jilotylc@state.gov");
		receivers.add("valmorolj@state.gov");

		ResultSet rs = stmt.executeQuery(sql );
		while ( rs.next() ) {
			String to = rs.getString("MetadataTo");
			String from = rs.getString("MetadataFrom");
			if(from.equals("H")&&receivers.contains(to.toLowerCase())){
				String emailBody = rs.getString("ExtractedBodyText");
				String emailSubject = rs.getString("ExtractedSubject");
				String tokens[] = tokenizer.tokenize(emailBody);
				for (String w : tokens){
					if(!isNotLetter(w)&&!stopWords.contains(w.toLowerCase())){
						w = w.replaceAll("[-+^,'\";!?{}]","");
						bodySet.add(w);
					}
				}
				String subTokens[] = tokenizer.tokenize(emailSubject);
				for (String w : subTokens){
					if(!isNotLetter(w)&&!stopWords.contains(w.toLowerCase())){
						w = w.replaceAll("[-+^,'\";!?{}]","");
						subjectSet.add(w);
					}
				}
			}

		}
		rs.close();

		int idx = 0;
		for(String w:bodySet){
			bodyDict.put(w, idx);
			idx++;
			bodyFeatures.add(w);
		}

		for(String w:subjectSet){
			subjectDict.put(w, idx);
			idx++;
			subjectFeatures.add(w);
		}
	}

	public void getWordCount(List<String> dictList) throws Exception {
		PrintWriter writer = new PrintWriter(featurePath, "UTF-8");		
		ResultSet rs = stmt.executeQuery(sql);
		while ( rs.next() ) {
			String emailBody = rs.getString("ExtractedBodyText");
			String tokens[] = tokenizer.tokenize(emailBody);
			Map<String, Integer> freq = new HashMap<String, Integer>();
			for (String a : tokens){
				if(freq.get(a)==null){
					freq.put(a, 1);
				} else {
					int ct = freq.get(a);
					freq.put(a, ct++);
				}
			}
			for (String k:dictList){
				if(freq.get(k)==null){
					writer.println(0);
				} else {
					writer.println(freq.get(k));
				}
			}
		}
		rs.close();
		stmt.close();
		c.close();

		writer.close();


	}

	public void writeData() throws Exception {		
		PrintWriter dataWriter = new PrintWriter(new FileOutputStream(dataPath, false));

		ResultSet rs = stmt.executeQuery(sql);
		while ( rs.next() ) {
			String emailBody = rs.getString("ExtractedBodyText");
			String emailSubject = rs.getString("ExtractedSubject");
			String tokens[] = tokenizer.tokenize(emailBody);
			String subTokens[] = tokenizer.tokenize(emailSubject);

			int [] wordCount = new int[bodyDict.size()+subjectDict.size()];
			Arrays.fill(wordCount, 0);

			for (String w : tokens){
				Integer idx = bodyDict.get(w);
				if(idx!=null){
					wordCount[idx]++;
					if(wordCount[idx]>bodyMaxCount){
						bodyMaxCount = wordCount[idx];
					}
				}
			}
			for (String w : subTokens){
				Integer idx = subjectDict.get(w);
				if(idx!=null){
					wordCount[idx]++;
					if(wordCount[idx]>subMaxCount){
						subMaxCount = wordCount[idx];
					}
				}
			}
			for (int i = 0; i < wordCount.length; i++) {
				dataWriter.print(wordCount[i]);
				dataWriter.print(",");
			}
			// label
			dataWriter.print("1");

			dataWriter.print("\n");

		}
		rs.close();

		dataWriter.close();


	}

	public void writeFeatures() throws Exception {
		writeData();

		PrintWriter featureWriter = new PrintWriter(new FileOutputStream(featurePath, false));		

		featureWriter.println("@relation Email");
		featureWriter.println();

		String val = "{";
		for (int i = 0; i <= bodyMaxCount; i++) {
			if(i>0){
				val += ",";
			}
			val += i;
		}
		val += "}";

		for (Iterator iterator = bodyFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailBody="+ f + " " + val);
		}

		val = "{";
		for (int i = 0; i <= subMaxCount; i++) {
			if(i>0){
				val += ",";
			}
			val += i;
		}
		val += "}";

		for (Iterator iterator = subjectFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailSubject="+ f + " " + val);
		}

		featureWriter.println("@attribute Class {1,2}");
		featureWriter.println();
		featureWriter.println("@data");

		featureWriter.close();


	}

	public void writeFeatures1() throws Exception {

		PrintWriter featureWriter = new PrintWriter(new FileOutputStream(featurePath, false));		

		featureWriter.println("@relation Email");
		featureWriter.println();

		for (Iterator iterator = bodyFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailBody="+ f + " numeric");
		}

		for (Iterator iterator = subjectFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailSubject="+ f + " numeric");
		}

		for (int i = 0; i < 7; i++) {
			featureWriter.println("@attribute day="+ i + " {0,1}");
		}
		for (int i = 0; i < 6; i++) {
			featureWriter.println("@attribute timeframe="+ i + " {0,1}");
		}
		String classVar = "{";
		for (Iterator iterator = labelSet.iterator(); iterator.hasNext();) {
			String c = (String) iterator.next();
			classVar+=c+",";
		}
		classVar=classVar.substring(0, classVar.length()-1);
		classVar+="}";

		featureWriter.println("@attribute Class " + classVar);
		featureWriter.println();
		featureWriter.println("@data");

		ResultSet rs = stmt.executeQuery(sql);
		while ( rs.next() ) {
			String emailBody = rs.getString("ExtractedBodyText");
			String emailSubject = rs.getString("ExtractedSubject");
			String dateSent = rs.getString("ExtractedDateSent");
			String label = rs.getString("label");
			String tokens[] = tokenizer.tokenize(emailBody);
			String subTokens[] = tokenizer.tokenize(emailSubject);

			int [] wordCount = new int[bodyDict.size()+subjectDict.size()];
			Arrays.fill(wordCount, 0);

			for (String w : tokens){
				Integer idx = bodyDict.get(w);
				if(idx!=null){
					wordCount[idx]++;
				}
			}
			for (String w : subTokens){
				Integer idx = subjectDict.get(w);
				if(idx!=null){
					wordCount[idx]++;
				}
			}
			for (int i = 0; i < wordCount.length; i++) {
				featureWriter.print(wordCount[i]);
				featureWriter.print(",");
			}

			int [] timeArr = new int[6];
			Arrays.fill(timeArr, 0);
			int [] dayArr = new int[7];
			Arrays.fill(dayArr, 0);

			//Saturday, May 30 2009 11:59 PM
			//Sun Oct 25 11:13:172009
			//Monday, October 26, 2009 7:25 AM
			if(dateSent!=null&&dateSent.length()>0){
				if(dateSent.contains(",")){
					String[] parts = dateSent.split(",");
					for (int i = 0; i < parts.length; i++) {
						String d = parts[i].trim().toUpperCase();
						if(d.startsWith("SUN")){
							dayArr[0]=1;
						} else if(d.startsWith("MON")){
							dayArr[1]=1;
						} else if(d.startsWith("TUE")){
							dayArr[2]=1;
						} else if(d.startsWith("WED")){
							dayArr[3]=1;
						} else if(d.startsWith("TH")){
							dayArr[4]=1;
						} else if(d.startsWith("FRI")){
							dayArr[5]=1;
						} else if(d.startsWith("SAT")){
							dayArr[6]=1;
						} 
					}
					String[] timeParts = parts[parts.length-1].split(" ");
					if(timeParts.length>=2){
						for (int i = 0; i < timeParts.length; i++) {
							String t = timeParts[i];
							if(t.contains(":")){
								try {
									int hr = Integer.valueOf(t.split(":")[0].trim());
									int idx = hr/3;
									String apm = timeParts[i+1];
									if(apm.trim().equalsIgnoreCase("PM")){
										idx+=3;
									}
									timeArr[idx]=1;
								} catch (Exception e) {
								}

							}
						}
					}
				}

			}
			for (int i = 0; i < dayArr.length; i++) {
				featureWriter.print(dayArr[i]);
				featureWriter.print(",");
			}
			for (int i = 0; i < timeArr.length; i++) {
				featureWriter.print(timeArr[i]);
				featureWriter.print(",");
			}


			// label
			featureWriter.print(label);
			featureWriter.print("\n");

		}
		rs.close();
		featureWriter.close();

	}

	public void writeFeatures2() throws Exception {

		PrintWriter featureWriter = new PrintWriter(new FileOutputStream(featurePath, false));		

		featureWriter.println("@relation Email");
		featureWriter.println();

		for (Iterator iterator = bodyFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailBody="+ f + " numeric");
		}

		for (Iterator iterator = subjectFeatures.iterator(); iterator.hasNext();) {
			String f = (String) iterator.next();
			featureWriter.println("@attribute emailSubject="+ f + " numeric");
		}

		for (int i = 0; i < 7; i++) {
			featureWriter.println("@attribute day="+ i + " {0,1}");
		}
		for (int i = 0; i < 6; i++) {
			featureWriter.println("@attribute timeframe="+ i + " {0,1}");
		}
		String classVar = "{";
		for (Iterator iterator = labelSet.iterator(); iterator.hasNext();) {
			String c = (String) iterator.next();
			classVar+=c+",";
		}
		classVar=classVar.substring(0, classVar.length()-1);
		classVar+="}";

		featureWriter.println("@attribute Class {0,1,2}");
		featureWriter.println();
		featureWriter.println("@data");

		Set<String> receivers = new HashSet<String>();
		receivers.add("abedin, huma");
		receivers.add("abedinh@state.gov");
		receivers.add("jilotylc@state.gov");
		receivers.add("valmorolj@state.gov");

		ResultSet rs = stmt.executeQuery(sql);
		while ( rs.next() ) {
			String to = rs.getString("MetadataTo");
			String from = rs.getString("MetadataFrom");
			if(from.equals("H")&&receivers.contains(to.toLowerCase())){
				String emailBody = rs.getString("ExtractedBodyText");
				String emailSubject = rs.getString("ExtractedSubject");
				String dateSent = rs.getString("ExtractedDateSent");
				String tokens[] = tokenizer.tokenize(emailBody);
				String subTokens[] = tokenizer.tokenize(emailSubject);

				int [] wordCount = new int[bodyDict.size()+subjectDict.size()];
				Arrays.fill(wordCount, 0);

				for (String w : tokens){
					Integer idx = bodyDict.get(w);
					if(idx!=null){
						wordCount[idx]++;
					}
				}
				for (String w : subTokens){
					Integer idx = subjectDict.get(w);
					if(idx!=null){
						wordCount[idx]++;
					}
				}
				for (int i = 0; i < wordCount.length; i++) {
					featureWriter.print(wordCount[i]);
					featureWriter.print(",");
				}

				int [] timeArr = new int[6];
				Arrays.fill(timeArr, 0);
				int [] dayArr = new int[7];
				Arrays.fill(dayArr, 0);

				//Saturday, May 30 2009 11:59 PM
				//Sun Oct 25 11:13:172009
				//Monday, October 26, 2009 7:25 AM
				if(dateSent!=null&&dateSent.length()>0){
					if(dateSent.contains(",")){
						String[] parts = dateSent.split(",");
						for (int i = 0; i < parts.length; i++) {
							String d = parts[i].trim().toUpperCase();
							if(d.startsWith("SUN")){
								dayArr[0]=1;
							} else if(d.startsWith("MON")){
								dayArr[1]=1;
							} else if(d.startsWith("TUE")){
								dayArr[2]=1;
							} else if(d.startsWith("WED")){
								dayArr[3]=1;
							} else if(d.startsWith("TH")){
								dayArr[4]=1;
							} else if(d.startsWith("FRI")){
								dayArr[5]=1;
							} else if(d.startsWith("SAT")){
								dayArr[6]=1;
							} 
						}
						String[] timeParts = parts[parts.length-1].split(" ");
						if(timeParts.length>=2){
							for (int i = 0; i < timeParts.length; i++) {
								String t = timeParts[i];
								if(t.contains(":")){
									try {
										int hr = Integer.valueOf(t.split(":")[0].trim());
										int idx = hr/3;
										String apm = timeParts[i+1];
										if(apm.trim().equalsIgnoreCase("PM")){
											idx+=3;
										}
										timeArr[idx]=1;
									} catch (Exception e) {
									}

								}
							}
						}
					}

				}
				for (int i = 0; i < dayArr.length; i++) {
					featureWriter.print(dayArr[i]);
					featureWriter.print(",");
				}
				for (int i = 0; i < timeArr.length; i++) {
					featureWriter.print(timeArr[i]);
					featureWriter.print(",");
				}
				receivers.add("abedin, huma");
				receivers.add("abedinh@state.gov");
				receivers.add("jilotylc@state.gov");
				receivers.add("valmorolj@state.gov");

				// label
				int c = 0;
				if(to.equalsIgnoreCase("abedin, huma")||to.equalsIgnoreCase("abedinh@state.gov")){
					c=0;
				} else if(to.equalsIgnoreCase("jilotylc@state.gov")){
					c=1;
				} else if(to.equalsIgnoreCase("valmorolj@state.gov")){
					c=2;
				}
				featureWriter.print(c);
				featureWriter.print("\n");

			}
			



		}
		rs.close();
		featureWriter.close();

	}

	private boolean isNotLetter(String c) {
		if(c.length()>1) return false;
		Pattern p = Pattern.compile("[^a-z]", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(c);
		boolean b = m.find();
		return b;
	}

	private boolean isSpecialChar(String c) {
		if(c.length()>1) return false;
		Pattern p = Pattern.compile("[^a-z0-9]", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(c);
		boolean b = m.find();
		return b;
	}

	public static void main( String args[] ) {
		try {
//			HillaryExplorer explorer = new HillaryExplorer();
//			explorer.writeFeatures2();
//			explorer.closeConnection();
			
			String sent2 = "Will I win the lottery at 2:30 http://v.youku.com/v_show/id_XNDM4NjYzMzU2.html?from=y1.6-96.1.1.5590af86bce011e0bf93?";
		    TokenizerFactory<CoreLabel> tokenizerFactory = 
		      PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		    List<CoreLabel> rawWords2 = tokenizerFactory.getTokenizer(new StringReader(sent2)).tokenize();
		    for (Iterator iterator = rawWords2.iterator(); iterator.hasNext();) {
				CoreLabel coreLabel = (CoreLabel) iterator.next();
				System.out.println(coreLabel);
			}
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		System.out.println("Operation done successfully");
	}
}
