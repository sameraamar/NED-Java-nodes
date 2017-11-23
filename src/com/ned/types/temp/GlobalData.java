package com.ned.types.temp;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.ned.types.Document;
import com.ned.types.DocumentWordCounts;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class GlobalData {

	public class Parameters 
	{
		public int roll_file = 1_000_000;
		public String DELIMITER = "\t";
		public int monitor_timer_seconds = 1; //seconds
		public int number_of_threads =100;
		public int print_limit = 5000;
		public int number_of_tables = 70;
		public int hyperplanes = 13; // k  -->  2^k * 2000 --> 
		public int max_bucket_size = 2000;
		public int max_documents = 30_000_000;
		public int max_thread_delta_time = 1*3600; //seconds
		public int offset =  0;
		public int provider_buffer_size = 25000; //read documents ahead
		public int search_recents = 2000;
		public double threshold = 0.5;
		public double min_cluster_entropy = 0.0;
		public double min_cluster_size = 1;
		public int inital_dimension = 100000;
		public int dimension_jumps = 100000;
		public boolean resume_mode = false;
		public boolean scan_mode_only = false; //keep this false unless you only wants to be in scan mode
	} 
	
	private static GlobalData globalData = null;
	public static GlobalData getInstance() 
	{
		if (globalData == null)
		{
			globalData = new GlobalData();
		}
		return globalData;
	}	
	
	public static void release()
	{
		globalData = null;
	}

	private Parameters mParam;
	
	
	private GlobalData()
	{	
		mParam = new Parameters();
	}
	
	synchronized public void init() throws Exception
	{
		
	}

	public Parameters getParams() {
		return mParam;
	}

	/***********************************************************************/
	
	public RedisBasedMap<String, Document>   id2doc;
	public RedisBasedMap<String, Integer>    word2index;
	public RedisBasedMap<String, DocumentWordCounts> id2wc;
	
	
	

	private int wordCounts(List<String> list, Map<Integer, Integer> map)
	{
		int max_idx = addWords(list);
		
		for (String w : list) 
		{
			int idx = word2index.get(w);
			int val = map.getOrDefault(idx,  0);
			val += 1;
			map.put(idx, val);
		}
		
		return max_idx;
	}
	
	private int addWords(List<String> list)
	{
		int max = 0;
		
		for (String w : list) 
		{
			int tmp = addWord(w);
			if (tmp > max)
				max = tmp;
		}
		
		return max;
	}
	
	private int addWord(String word)
	{
		int idx = word2index.getOrDefault(word, -1);
		if (idx == -1) 
		{
			synchronized (word2index)
			{
				if (word2index.getOrDefault(word, -1) == -1)
				{					
					word2index.put(word, idx);
				}
			}
		}
		
		return idx;
	}
	
	public int addDocument(Document doc, int idx) 
	{
		
		DocumentWordCounts dwc = doc.bringWordCount();
		if(dwc == null)
		{
			String id = doc.getId().intern();
			synchronized (id) {
				dwc = id2wc.getOrDefault( doc.getId(), new DocumentWordCounts(id, new HashMap<Integer, Integer>()) );
				id2wc.put(id,  dwc);
			}
		}
		
		int d = wordCounts( doc.getWords(), dwc.getWordCount() );
		
		id2doc.put(doc.getId(), doc);

		addToRecent(doc.getId());
		
		return d;
	}
	
}
