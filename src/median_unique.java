import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.HashMap;

public class median_unique {

	/*NOTE: PriorityQueue w/ min/max heap is easy to implement but slow at O(log(n)) insertion time and memory heavy (O(n)).
	 * At 140 max characters there are a max of 70 possible unique words (letter alternating with a space delimiter)
	 * per tweet. Treat the "sorted list" of unique word frequencies as a histogram. Use an array where each array index 
	 * represents unique word count and long # represents the frequency of that count in tweets.txt. As if the numbers
	 * were in a sorted list, keep track of the range of "indices" which equal the current median unique word count. If 
	 * the median index is outside the current range, adjust the median unique word count and shift the range of indices. 
	 * This gives O(1) insertion/median return time per tweet and constant memory usage, far better than using PriorityQueue.
	 *
	 * Another plus with this implementation is that it is much easier (in terms of speed and code) to visualize the 
	 * overall distribution of unique word count frequencies. 
	 */
	
	public static void main(String[] args) throws Exception {
		
		long time = System.currentTimeMillis();
		
		// path to folder containing run.sh 
		String runDirectory = args[0];
		
		//histogram with 70 unique word count indices, frequencies represented by long
		int maxWords = 70;
		long[] uniqueWordsHistogram = new long[maxWords];
		
		int uniqueWords = 0;
		
		//the index in the array representative of a given unique word count
		int medianWordCount = 0;
		
		//equal to # of tweets - 1 to account for 0th index
		double tweetCount = 0;
		
		//defined as tweetCount / 2, ends in .5 to indicate when median index is between indices
		double medianIndex = 0;
		
		//inclusive range of "indices" containing the current median unique word count
		long lowerIndexLimit = 0;
		long upperIndexLimit = 0;
		
		//read input from tweets.txt
		String tweetFile = runDirectory + "/tweet_input/tweets.txt";
		File file = new File(tweetFile);
		FileInputStream fi = new FileInputStream(file);
		BufferedInputStream bi = new BufferedInputStream(fi);
		
		/*NOTE: using BufferedInputStream to read in file all at once is much faster than BufferedReader.
		 * Separating reading and parsing actions saves a lot of time in I/O heavy tasks like this one.
		 */
		
		//read entire file into single byte array
		int fileSize = (int) file.length();
		byte[] bytes = new byte[fileSize];
		bi.read(bytes, 0, fileSize);
		
		bi.close();
		fi.close();
		
		//UTF-8 is backwards compatable with ASCII, add newline for parsing purposes
		String fileString = new String(bytes, "UTF-8");
		bytes = null;
		fileString = fileString + "\n";
		
		//designate output file as ft2.txt, writing time can't be improved much
		String ft2 = runDirectory + "/tweet_output/ft2.txt";
		File output = new File(ft2);
		FileWriter fw = new FileWriter(output);
		BufferedWriter bw = new BufferedWriter(fw);
		
		//read in the first tweet, hold tweetCount at 0 to account for 0th index
		int index = fileString.indexOf('\n');
		String firstTweet = fileString.substring(0, index);
		uniqueWords = uniqueWordCount(firstTweet);
		uniqueWordsHistogram[uniqueWords]++;
		medianWordCount = uniqueWords;
		bw.write(medianWordCount + ".00");
		bw.newLine();
	
		//pos is substring starting index, end is substring end index, index is end of firstTweet
		int pos = index + 1, end;
		String line;
		
		/*NOTE: using a custom 'indexOf' parser is much faster than both String.split() and Guava Splitter.
		 * Using nested loop for '\n' and ' ' parsing is much faster than using one loop after str.replace('\n',' ')
		 */
		
		//for each tweet: count unique words, increment unique word count index in histogram, find median and output it
		while ((end = fileString.indexOf('\n', pos)) >= 0) {
			tweetCount++;
			line = fileString.substring(pos, end);
			uniqueWords = uniqueWordCount(line);
			uniqueWordsHistogram[uniqueWords]++;
			
			//tweet's unique word count is less than the median
			if (uniqueWords < medianWordCount){
				
				//shift median "index range" up by one
				lowerIndexLimit++;
				upperIndexLimit++;
				
				medianIndex = tweetCount / 2;
				
				//median index is inside the index range for the median word count
				if(medianIndex >= lowerIndexLimit){
					bw.write(medianWordCount + ".00");
					bw.newLine();
				}
				
				//median index is outside the range, adjust median word count and shift index range
				else if (medianIndex == lowerIndexLimit - 1){
					
					//if the frequency is zero at that index, keep shifting the index
					do{
						medianWordCount--;
					}while(uniqueWordsHistogram[medianWordCount] == 0);
					
					upperIndexLimit = lowerIndexLimit - 1;
					lowerIndexLimit = lowerIndexLimit - uniqueWordsHistogram[medianWordCount];
					
					bw.write(medianWordCount + ".00");
					bw.newLine();
				}
				
				//median index is between indices on histogram, return average of word counts
				else{
					int tempWordCount = medianWordCount;
					
					do{
						tempWordCount--;
					}while(uniqueWordsHistogram[tempWordCount] == 0);
					
					bw.write( ( (double) (medianWordCount + tempWordCount)) / (double) 2 + "0");
					bw.newLine();
				}
			}
			
			//tweet's unique word count is greater than the median
			else if (uniqueWords > medianWordCount){
				medianIndex = tweetCount / 2;
				
				//median index is inside the index range for the median word count
				if(medianIndex <= upperIndexLimit){
					bw.write(medianWordCount + ".00");
					bw.newLine();
				}
				
				//median index is outside the range, adjust median word count and shift index range
				else if (medianIndex == upperIndexLimit + 1){
					
					do{
						medianWordCount++;
					}while(uniqueWordsHistogram[medianWordCount] == 0);
					
					lowerIndexLimit = upperIndexLimit + 1;
					upperIndexLimit = upperIndexLimit + uniqueWordsHistogram[medianWordCount];
					
					bw.write(medianWordCount + ".00");
					bw.newLine();
				}
				
				//median index is between indices on histogram, return average of word counts
				else{
					int tempWordCount = medianWordCount;
					
					do{
						tempWordCount++;
					}while(uniqueWordsHistogram[tempWordCount] == 0);
					
					bw.write( ( (double) (medianWordCount + tempWordCount)) / (double) 2 + "0");
					bw.newLine();
				}
			}
			
			//tweet's unique word count equals the median word count
			else {
				upperIndexLimit++;
				bw.write(medianWordCount + ".00");
				bw.newLine();
			}
			pos = end + 1;
		}
		
		bw.close();
		
		System.out.println("execution time in ms: " + (System.currentTimeMillis() - time));
		
		
	}
	
	//outputs number of words in string using space delimiter
	public static int uniqueWordCount (String line){
		
		//add space for parsing purposes
    		line = line + " ";
    	
		//memory is cheap, go for low lookup cost to boost speed
    		int capacity = 300;
		HashMap<String, String> hm = new HashMap<String, String>(capacity);
 		
		String word;
		int pos = 0, end;
		
		while ((end = line.indexOf(' ', pos)) >= 0) {
			
			//put parsed word in HashMap, overwrite if key exists
			word = line.substring(pos, end);
			hm.put(word, "");
			
			pos = end + 1;
		}
		
        return hm.size();
	}
	
}
