import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class words_tweeted {
	
	public static void main(String[] args) throws Exception {
		
		long time = System.currentTimeMillis();  
		
		// path to folder containing run.sh 
		String runDirectory = args[0];
		
		/*NOTE: Assume all tweets being processed are in English. Global Language Monitor reports there are
		 *  ~1,000,000 English words. Memory is cheap so boost speed by lowering lookup cost i.e. use low load factor.
		 *  cache misses per lookup fall by ~25% from load of .75 to load of .2:
		 *  https://en.wikipedia.org/wiki/Hash_table#/media/File:Hash_table_average_insertion_time.png
		 */
		
		//Thread array of size N + 1 where N is the number of computer processors
		int threadNum = 5;
		
		//~50 MB when full	
		int capacity = 5000000;
		ConcurrentHashMap<String, Counter> hm = new ConcurrentHashMap<String, Counter>(capacity, .75f, threadNum);

		/*NOTE: using BufferedInputStream to read in file all at once (or large chunks at a time) is much faster than
		 * BufferedReader. Separating reading and parsing actions saves a lot of time in I/O heavy tasks like this one.
		 */

		//read input from tweets.txt
		String tweetFile = runDirectory + "/tweet_input/tweets.txt";
		File file = new File(tweetFile);
		FileInputStream fi = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fi);
		
		//for outputting formatted word/count to text file
		String ft1 = runDirectory + "/tweet_output/ft1.txt";
		File output = new File(ft1);
		FileWriter fw = new FileWriter(output);
		BufferedWriter bw = new BufferedWriter(fw);

		//max batch import size in MB, decrease the value if you are getting "OutOfMemoryError" and
		//increasing JVM max heap size is impossible
		long importSizeLimit = 2000;
		
		//max array length
		int arrayMaxSize = (int) Math.min(Integer.MAX_VALUE - 5, 1000000*importSizeLimit);
		
		//text file's size in bytes
		long fileSize = (int) file.length();
		
		//number of individual imports
		int numberOfBisReads = (int) (fileSize / arrayMaxSize) + 1;
		
		long bytesRemaining = fileSize;
		
		for(int i = 0; i < numberOfBisReads; i++){
		
			//read entire file (or section of file) into single byte array
			int arraySize = (int) Math.min(arrayMaxSize, bytesRemaining);
			byte[] bytes = new byte[arraySize];
			bis.read(bytes, 0, arraySize);
			
			//convert byte[] to String, UTF-8 is backwards compatable with ASCII
			String fileString = new String(bytes, "UTF-8");
			bytes = null;
			
			Consumer[] c = new Consumer[threadNum];
			Thread[] consumers = new Thread[threadNum];
			
			//subString indecies
			int start = 0;
			int end = 0;
			String subString;
			
			//split fileString into substrings for parallel processing
			for(int j = 0; j < threadNum; j++){
				
				end = ( ((int)arraySize * (j + 1)) / threadNum);
				end = Math.max(end, fileString.indexOf(' ', end));
				
				//add newline for parsing purposes
				subString = fileString.substring(start, end);
				subString = subString + "\n";
				
				c[j] = new Consumer(hm, subString);
				consumers[j] = new Thread(c[j]);
				consumers[j].start();
				
				start = end + 1;
			}
			
			for(int j = 0; j < consumers.length; j++){
				consumers[j].join();
			}
			
			/*NOTE: Using HashMap w/ merge sort is preferable to using TreeMap w/o merge sort.
			 * TreeMap gives O(log(n)) put/get/containsKey time but its really t*O(log(n)) where t is the number
			 * of tweeted words; HashMap gives t*O(1) put/get/containsKey time with only one merge sort at O(n*log(n)).
			 * Because the number of tweet words, t, is much larger (5*10^8 tweets per day * words/tweet) than the size of the  
			 * HashMap, n (there are only 10^6 english words), it would be far slower to use the TreeMap.
			 * t*O(1) + O(n*log(n)) << t*O(log(n)); therefore HashMap w/ merge sort is faster.
			 *
			 * Also note that while merge sort is a prime candidate for multithreading, becuase our HashMap is
			 * small, resulting performance gains are negligible.
			 * https://courses.cs.washington.edu/courses/cse373/13wi/lectures/03-13/MergeSort.java
			 */
			
			//sort the HashMap values by ASCII code
			List<String> sortedKeys = new ArrayList<String>(hm.keySet());
			Collections.sort(sortedKeys);
			
			//format and print words and counts
			Iterator<String> it = sortedKeys.iterator();
			for (int j = 0; j < sortedKeys.size(); j++){
				String s = it.next();
				bw.write(String.format("%-30s%d", s, hm.get(s).returnCount()));
				bw.newLine();
			}

			bytesRemaining = bytesRemaining - arraySize;
			
		}
		
		bis.close();
		fi.close();
		bw.close();
		
		System.out.println("execution time in ms: " + (System.currentTimeMillis() - time));
		
		
	}
	
	//Originally designed to work in producer/consumer model but after implementing BufferedInputStream,
	//task is no longer I/O-bound, producer thread was no longer needed; this consumer class is made for
	//parallel processing of parsing and HashMap operations
	public static class Consumer implements Runnable {
		ConcurrentHashMap<String, Counter> map;
		String subString;
		
		Consumer(ConcurrentHashMap<String, Counter> hm, String s) { 
			map = hm;
			subString = s;
		}

		public void run() {

			try {
		
				//pos is substring starting index, end is substring ending index
				int pos1 = 0, end1;
				String line;
				String word;
				
				/*NOTE: using a custom 'indexOf' parser is much faster than both String.split() and Guava Splitter.
				 * Using nested loop for '\n' and ' ' parsing is much faster than using one loop after str.replace('\n',' ')
				 */
					
				while ((end1 = subString.indexOf('\n', pos1)) >= 0) {
					
					//space added to line for parsing purposes
					line = subString.substring(pos1, end1);
					line = line + " ";
					int pos2 = 0, end2;
					
					while ((end2 = line.indexOf(' ', pos2)) >= 0) {
						word = line.substring(pos2, end2);
						
						//if Hashmap contains word, increment count			
						if (map.containsKey(word)){
							map.get(word).incrementCount();
			        		}
			        	
			        		//if word not found, insert new word into HashMap w/ count of 1
			        		else{
			        			map.put(word, new Counter());
			        		}
						
						pos2 = end2 + 1;
					}
					pos1 = end1 + 1;
				}
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	//counter class to increment word occurence count
	public static class Counter{
		
		int count;
		
		public Counter(){
			this.count = 1;
		}
		
		public void incrementCount(){
			this.count++;
		}
		
		public int returnCount(){
			return this.count;
		}
	}

}
