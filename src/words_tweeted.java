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
		ConcurrentHashMap<String, Counter> hm = new ConcurrentHashMap<String, Counter>(capacity, .75f, threadNum - 1);

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
		int arrayMaxSize = (int) Math.min(Integer.MAX_VALUE - 6, 1000000*importSizeLimit);
		
		//text file's size in bytes
		long fileSize = (int) file.length();
		
		//number of individual imports
		int numberOfBisReads = (int) (fileSize / arrayMaxSize) + 1;
		
		long bytesRemaining = fileSize;
		
		for(int i = 0; i < numberOfBisReads; i++){
		
			//read entire file (or section of file) into single byte array, add space for parsing purposes
			int arraySize = (int) Math.min(arrayMaxSize, bytesRemaining);
			byte[] bytes = new byte[arraySize + 1];
			bis.read(bytes, 0, arraySize);
			bytes[arraySize] = (byte) ' ';
			
			Consumer[] c = new Consumer[threadNum];
			Thread[] consumers = new Thread[threadNum];
			
			//subString indecies
			int start = 0;
			int end = 0;
			String subString;
			
			//partition array segments among threads for parallel processing
			for(int j = 0; j < threadNum; j++){
				
				end = ( ((int)arraySize * (j + 1)) / threadNum);
				
				while ( bytes[end] != (byte) ' ' ){
					end++;
				}
				
				c[j] = new Consumer(hm, bytes, start, end);
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
			 * Because the number of tweeted words, t, is much larger (5*10^8 tweets per day * words/tweet) than 
			 * the size of the HashMap, n (there are only 10^6 english words), it would be far slower to use the TreeMap.
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
		byte[] bytes;
		int startIndex;
		int endIndex;
		
		Consumer(ConcurrentHashMap<String, Counter> hm, byte[] b, int start, int end) { 
			map = hm;
			bytes = b;
			startIndex = start;
			endIndex = end;
		}

		public void run() {

			try {
		
				//pos is array starting index
				int pos = startIndex;
				byte space = (byte) ' ';
				byte newLine = (byte) '\n';
				
				//iterate through array segment parsing words and perform hashmap operations, this method is much faster 
				//than any string parsing method such as String.split, tokenizer, Guava splitter, indexOf() etc.
				for (int i = startIndex; i < endIndex + 1; i++){
					if (bytes[i] == space || bytes[i] == newLine){
						
						//generate string object of word from array segment
						byte[] temp = new byte[i - pos];
						System.arraycopy(bytes, pos, temp, 0, temp.length);
						String word = new String(temp, "UTF-8");
						
						/*NOTE: Although there is a possibility of one thread putting a value while the other thread is
						 * getting a null from that bucket thus causing an overwrite and decreasing the true count for that word by 1, 
						 * the overall chances of this happening are minimal, i.e. the sum of squared likelihoods of occurance for 
						 * each word during the first put of a that word. Performance gains of no synchronization far outweigh
						 * chances/effects of overwrite occuring.
					         */
						
						//if hashmap contains word, increment count
						if (map.containsKey(word)){
							map.get(word).incrementCount();
						}
			        	
						//if word not found, insert new word into HashMap w/ count of 1
						else{
							map.put(word, new Counter());
						}
						
						pos = i + 1;
						
					}
					
				}
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	//counter class to increment word occurrence count
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
