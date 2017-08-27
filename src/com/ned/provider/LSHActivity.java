package com.ned.provider;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ned.types.Document;
import com.ned.types.Tupel2;
import com.ned.types.temp.GlobalData;

import ned.hash.LSHForest;

public class LSHActivity extends ActivityBase<Tupel2<String, List<String>>> {

	private LSHForest forest;
	Yieldery<Tupel2<Document, Integer>> docList;

	public LSHActivity(Yieldery<Tupel2<Document, Integer>> input) 
	{
		this.docList = input;
	}
	
	@Override
	protected void doRun() {
		
		while(!docList.isDone())
		{
			Tupel2<Document, Integer> doc = docList.poll();
			while(doc == null)
			{
				safeSleep(10);
				doc = docList.poll();
			}

			int dimension = GlobalData.getInstance().addDocument(doc._1, doc._2);
			
			List<String> set = new LinkedList<String>();
			
			if (doc._1.getWords().size() > 0)
			{
		    	Map<Integer, Double> word2idf = new Hashtable<Integer, Double>();
	    	
				//JedisPool jedisPool = RedisAccessHelper.createRedisConnectionPool();
		    	//GlobalData.getInstance().thread2redis.put(Thread.currentThread().getName(), jedisPool);
		    	
		    	set = forest.addDocument(doc._1, dimension, word2idf);
	
		    	//DocumentClusteringHelper.postLSHMapping(this.doc, set, word2idf);
		    	//doc._1.setNearestDetermined( true);
			}
			
			getBus().add(new Tupel2<String, List<String>>(doc._1.getId(), set));
			
			int counter = getBus().getCounter();
			if(counter % 10000 == 0)
			{
				System.out.println(this.getClass().getSimpleName() + ": " + counter);
			}
		}
		getBus().isDone(true);

		System.out.println(this.getClass().getSimpleName() + ": done.");
		
	}

	@Override
	protected void initHook() {
		GlobalData gd = GlobalData.getInstance();
		forest = new LSHForest(gd.getParams().number_of_tables, 
				 gd.getParams().hyperplanes, 
				 gd.getParams().inital_dimension, 
				 gd.getParams().max_bucket_size);
	}

}
