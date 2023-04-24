import java.io.*;
import java.nio.file.*;
//import org.hyperic.sigar.CpuPerc;
//import org.hyperic.sigar.Sigar;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class App {	
	public static final int TOP_K = 10;
	public static final Set<String> stopset = new HashSet<>(Arrays.asList("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now"));
    
	public static void hashmap(String filename) {

		 try {
		  
		        Path path = Paths.get(filename);
		        BufferedReader reader = Files.newBufferedReader(path);
	            HashMap<String,Integer> map = new HashMap<>();
	            //Reading line by line from the input text file
	            String line;
	            while ((line = reader.readLine()) != null) {
	                String[] words = line.split("\\W+");
	                for (String word : words) {
	                	String lower= word.toLowerCase().trim();
	                	map.put(lower, map.getOrDefault(lower, 0) + 1);
	                }

	            }
	      
	            // Sorting the map entries using list and comparator
	            List<Map.Entry<String, Integer>> sortedlist = new ArrayList<>(map.entrySet());
	            Collections.sort(sortedlist, new Comparator<Map.Entry<String, Integer>>() {
	                public int compare(Map.Entry<String, Integer> word1, Map.Entry<String, Integer> word2) {
	                    int freqdiff = word2.getValue().compareTo(word1.getValue());
	                    return freqdiff!=0 ? freqdiff : word1.getKey().compareTo(word2.getKey());
	                }
	            });
	            Iterator<Map.Entry<String, Integer>> iterator = sortedlist.iterator();
	            for (int i = 0; i < TOP_K && iterator.hasNext(); ) {
	            	Map.Entry<String, Integer> outputword = iterator.next();
				
				 if(! stopset.contains(outputword.getKey()) ) {
					 	System.out.println(outputword);
					 	 i++;
					}
	            }
	            
	            reader.close();


	        } catch (Exception e) {
	        	 System.out.println("File not found");
	        }
	 
	        
	}
	
	
	public static void processfile(String chunk, Map<String, Integer> map) {
	    String[] words = chunk.split("\\W+");
	    for (String word : words) {
	    	String lower= word.toLowerCase();
	    	map.put(lower, map.getOrDefault(lower, 0) + 1);
	        }
	    }
	public static void Priorityqueue(String filename) {
	         HashMap<String,Integer> map = new HashMap<>();    
	         int CHUNK_SIZE=100000;  
	         try {
	        	 File file = new File(filename);
	        	 //Reading in chucks 
	        	 	BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file))));
	        	 	char[] buffer = new char[CHUNK_SIZE];
	        	    StringBuilder temp = new StringBuilder();
	        	    int charread; 
	        	    while ((charread = reader.read(buffer, 0, CHUNK_SIZE)) != -1) {
	        	    	temp.append(buffer, 0, charread); 
	        	    	int lastWhitespace = temp.lastIndexOf(" ");
	                    if (lastWhitespace != -1) {
	                        String chunk = temp.substring(0, lastWhitespace);
	                        temp.delete(0, lastWhitespace + 1);	               
	                        processfile(chunk, map);
	        	        }
	        	    }
	        	    if (temp.length() > 0) {
	        	    	processfile(temp.toString(), map);
	        	    }
      
	         //Min heap for storing exactly K elements, saving memory
	         PriorityQueue<Map.Entry<String, Integer>> min_heap = new PriorityQueue<>(TOP_K+1, new Comparator<Map.Entry<String, Integer>>() {
	             public int compare(Map.Entry<String, Integer> word1, Map.Entry<String, Integer> word2) {
	                 int freqdiff = word1.getValue().compareTo(word2.getValue());
	                 return freqdiff!=0 ? freqdiff : word2.getKey().compareTo(word1.getKey());
	             }
	         });

	         //Stores K elements, deletes minimum frequency and filters stopword
	         for (Map.Entry<String, Integer> outputword : map.entrySet()) {  
	        	  if(! stopset.contains(outputword.getKey())) {	           
		            	min_heap.add(outputword);
		            if(min_heap.size() > TOP_K) {
		            	min_heap.poll();
		            }
		        }
	         }
	         
	         //Reversing the as min- heap is used
	         List<Map.Entry<String, Integer>> FreqWords = new ArrayList<>();
	         while (!min_heap.isEmpty() && FreqWords.size() < TOP_K) {
	        	 FreqWords.add(min_heap.poll());
	         }
	         Collections.reverse(FreqWords);
	         System.out.println(FreqWords);
	         reader.close();
		 }catch (Exception e) {
	         System.out.println("File not found");
	     }}
		
	
	
	
    public static void main(String[] args) throws Exception  {
    	Runtime runtime = Runtime.getRuntime();
    	long membefore = runtime.totalMemory()-runtime.freeMemory();
    	long startTime = System.nanoTime();
        System.out.println("startTime " + startTime);
         
        String filename = "S:\\Sushmitha\\Course Materials\\Bigdata\\Assignments\\Assignment 1\\dataset_updated\\data_2.5GB.txt";
        
        hashmap(filename);
        //Priorityqueue(filename);

        long memafter = runtime.totalMemory()-runtime.freeMemory();
        long endTime = System.nanoTime();
        System.out.println("endTime " + endTime);
        System.out.println("Execution Time: " + (endTime-startTime) + " ns "  + (endTime-startTime) / 1000000L + " ms " + (endTime-startTime) / 1000000000L + " sec");
        System.out.println("Memory Usage " + (memafter-membefore)/1024 +" KB " + (memafter-membefore)/(1024*1024) +" MB");
    }
}
