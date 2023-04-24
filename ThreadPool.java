import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ThreadPool implements Callable<Void>{
    private static final int NUM_THREADS = 8;
    private static final int CHUNK_SIZE = 1024 * 1024 *10 ; // 10 MB
    private static final int TOP_K = 10;
    public static ConcurrentHashMap<String, Integer> result = new ConcurrentHashMap<>();
    public static final Set<String> stopset = new HashSet<>(Arrays.asList("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now"));
    private static PriorityQueue<Map.Entry<String, Integer>> gettopwords(ConcurrentHashMap<String, Integer> wordFreq, int k) {
        PriorityQueue<Map.Entry<String, Integer>> topKWords = new PriorityQueue<>(k+1, new Comparator<Map.Entry<String, Integer>>() {
             public int compare(Map.Entry<String, Integer> word1, Map.Entry<String, Integer> word2) {
                 int freqdiff = word1.getValue().compareTo(word2.getValue());
                 return freqdiff!=0 ? freqdiff : word2.getKey().compareTo(word1.getKey());
             }
         });
        // Adding the words from the ConcurrentHashMap to the priority queue
        for (Map.Entry<String, Integer> entry : wordFreq.entrySet()) {
      	  if(! stopset.contains(entry.getKey())) {	           
      		topKWords.add(entry);
          if(topKWords.size() > k) {
        	  topKWords.poll();
          }}
        }

        return topKWords;
    }
    public static void main(String[] args) throws Exception {
    	long startTime = System.nanoTime();
    	System.out.println("startTime " + startTime);
    	 Runtime runtime = Runtime.getRuntime();
      	long membefore = runtime.totalMemory()-runtime.freeMemory();
        String filename = "S:\\Sushmitha\\Course Materials\\Bigdata\\Assignments\\Assignment 1\\dataset_updated\\data_2.5GB.txt";

        // Determine the number of chunks to split the file
        long fileSize = new File(filename).length();
        int numChunks =1;
        if(fileSize > numChunks)
          numChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);

        // Process each chunk of the file in a separate thread
        for (int i = 0; i < numChunks; i++) {
            int chunkNumber = i;
            Callable<Void> threadcall = new ThreadPool(filename, chunkNumber);
            threadPool.submit(threadcall);
        }

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        PriorityQueue<Map.Entry<String, Integer>> topKWords = gettopwords(result, TOP_K);

        // Print the top K most frequent words
        for (Map.Entry<String, Integer> wf : topKWords) {
            System.out.println(wf.getKey() + ": " + wf.getValue());
        }
        long endTime = System.nanoTime();
        long memafter = runtime.totalMemory()-runtime.freeMemory();
        System.out.println("endTime " + endTime);
        System.out.println("Execution Time: " + (endTime-startTime) + " ns "  + (endTime-startTime) / 1000000L + " ms " + (endTime-startTime) / 1000000000L + " sec");
        System.out.println("Memory Usage " + (memafter-membefore)/1024 +" KB " + (memafter-membefore)/(1024*1024) +" MB");
    }

	 private final String filename;
     private final int chunkNumber;

     public ThreadPool(String filename, int chunkNumber) {
         this.filename = filename;
         this.chunkNumber = chunkNumber;
     }
	
     public Void call() throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            // Determine the start and end positions of the chunk
            long startPos = (long) chunkNumber * CHUNK_SIZE;
            long endPos = startPos + CHUNK_SIZE;
            if (endPos > raf.length()) {
                endPos = raf.length();
            }
            raf.seek(startPos);
            byte[] buffer = new byte[CHUNK_SIZE];
            int numBytesRead = raf.read(buffer);
            String chunk = new String(buffer, 0, numBytesRead);
            processText(chunk, result);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private static void processText(String text, ConcurrentHashMap<String, Integer> wordFreq) {
    	ConcurrentHashMap<String, Integer> chunkWordFreq = new ConcurrentHashMap<>();
        String[] words = text.split("\\W+");
        for (String word : words) {
            if (!word.isEmpty()) {
            	chunkWordFreq.merge(word.toLowerCase(), 1, Integer::sum);
             }
        }
        chunkWordFreq.forEach((key, value) -> wordFreq.merge(key,value,Integer::sum));
    } 
}
