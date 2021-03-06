package com.ned.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

final public class Utility {
	private static Random rand = new Random(1000000);

	private Utility()
	{
	}
    
	public static double randomFill(){
        double randomNum = 2.0 * rand.nextDouble() - 1.0;
        return randomNum;
    }
    
	public static String humanTime(long seconds)
	{
		long diff[] = new long[] { 0, 0, 0, 0 };
	    /* sec */diff[3] =   (seconds >= 60 ? seconds % 60 : seconds);
	    /* min */diff[2] =   (seconds = (seconds / 60)) >= 60 ? seconds % 60 : seconds;
	    /* hours */diff[1] = (seconds = (seconds / 60)) >= 24 ? seconds % 24 : seconds;
	    /* days */diff[0] =  (seconds = (seconds / 24));

	    /*
	    String text = "";
	    if (diff[0] > 1)
	    	text += "%d d%s";
	    if (diff[1] > 1)
	    	text += "%d h%s";
	    if (diff[1] > 1)
	    	text += "%d m%s";
	    if (diff[1] > 1)
	    	text += "%d s%s";
	    */
	    
	    String text = String.format(
		        //"%d hour%s, %d minute%s, %d second%s",
		        "%d days %02d:%02d:%02d",
	        diff[0],
	        //diff[0] > 1 ? "s" : "",
	        diff[1],
	        //diff[1] > 1 ? "s" : "",
	        diff[2],
	        //diff[2] > 1 ? "s" : "",
	        diff[3]
	        //diff[3] > 1 ? "s" : ""
	        );
	    
	    return text;
	}

	 public static <K, V> Set<K> intersection(Map<K, V> left, Map<K, V> right) 
	 {
		 if (left.size() > right.size())
		 {
			 Map<K, V> tmp = right;
			 right = left;
			 left = tmp;
		 }
       
		 HashSet<K> intersection = new HashSet<K>();
		 Set<K> lkeys = left.keySet();
		 for (K key : lkeys) 
		 {
			 if (right.containsKey(key))
			 {
				 intersection.add(key);
			 }
		 }

		 return intersection;
	 }
	 
	 public static <K, V> Set<K> intersection(HashMap<K, V> left,HashMap<K, V> right) {
		 if (left.size() > right.size())
	        {
			 	HashMap<K, V> tmp = right;
				right = left;
				left = tmp;
	        }
	        
	        HashSet<K> intersection = new HashSet<K>();
	        Set<Entry<K, V>> lkeys = left.entrySet();
	        
	        for (Entry<K, V> entry : lkeys) {
	        	K k = entry.getKey();
	        	if (right.containsKey(k))
	            {
					intersection.add(k);
	            }
			}
	       

	        return intersection;
	    }
	
}
