package com.ned.types;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ned.types.temp.GlobalData;
import com.ned.util.DirtyBit;

import com.ned.util.Twokenize;
import com.ned.util.Utility;

public class Document  implements Serializable, DirtyBit {
    /**
	 * 
	 */
	private static final long serialVersionUID = 4575371423244913253L;

	private static boolean isBasicOnly;
	
	public boolean isDirtyBit;
	
	private String id;
    private String text ;
    private List<String> words;
    //private int dimension;
    
    public int max_idx;
	private String cleanText;
	//document attributes
	private long timestamp;
	private String created_at;
	private String retweeted_id;
	private int retweet_count;
	private String reply_to;
	
	private int favouritesCount;
	private String user_id;
	private String retweeted_user_id;
	private String quoted_status_id;
	private String reply_to_user_id;
	private String quoted_user_id;
	
	private Document(String id)
	{
    	this.id = id.intern();
	}
	
    private void init(String text, long timestamp)
    {
        this.text = text;
        this.timestamp = timestamp;
        this.created_at = null;
    	this.retweeted_id = null;
    	this.retweet_count = 0;
    	this.favouritesCount = -1;
    	this.reply_to = null;
        this.words = Twokenize.tokenizeRawTweetText(text.toLowerCase());;
        this.cleanText = String.join(" ", words);
    }
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Document) 
		{
			String other = ((Document)obj).id;
			return this.id.equals(other);
		}
		
		return false;
	}
	
	@Override
	public int hashCode() 
	{
		return id.hashCode();
	}
	
    private static double Norm(Map<Integer, Double> rWeights) {
    	double res = 0;
        for(Double v : rWeights.values())
        {
            res += v * v;
        }

        res = Math.sqrt(res);
        return res;
	}
    
    public static double Distance(DocumentWordCounts left, DocumentWordCounts right, Map<Integer, Double> word2idf)
    {
    	Set<Integer> commonWords = Utility.intersection(left.getWordCount(), right.getWordCount());
     	if(commonWords.isEmpty()){
     		return 1;
     	}

        if (left.getWordCount().size() > right.getWordCount().size())
        {
            DocumentWordCounts tmp = right;
            right = left;
            left = tmp;
        }
        
        Map<Integer, Double> rWeights = right.getWeights(word2idf);
        Map<Integer, Double> lWeights = left.getWeights(word2idf);
        
        double res = 0;
        double norms = Norm(rWeights) * Norm(lWeights);
        double dot = 0.0;

        for (Integer k : commonWords) {
            dot += rWeights.get(k) * lWeights.get(k);
		}
        
        res = dot / norms; 
        return 1.0 - res;
     	 
    }

	public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("{").append(id).append(": ").append(text);
    	//sb.append(weights)
    	sb.append("}");
    	return sb.toString();
    }

	public String getText() {
		return text;
	}

	public List<String> getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public DocumentWordCounts bringWordCount()
	{
		DocumentWordCounts wordCount = GlobalData.getInstance().id2wc.get(id);
		return wordCount;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public String getCleanText() {
		return cleanText;
	}

	public void setCreatedAt(String created_at) {
		this.created_at = created_at;
		dirtyOn();
	}

	public void updateNearest(DocumentWordCounts rWordCount, Map<Integer, Double> word2idf) {
		if(rWordCount == null)
			return;
		
		if(rWordCount.getId().compareTo(getId()) >= 0)
			return;
		
		DocumentWordCounts myWC = GlobalData.getInstance().id2wc.get( getId() );
				
		double tmp = Document.Distance(myWC, rWordCount, word2idf);
		if (getNearestId()==null || tmp < getNearestDist())
		{
			setNearestDist(tmp);
			setNearestId( rWordCount.getId() );	
		}
	}
	public void updateNearest(String rightId, Map<Integer, Double> word2idf) 
	{
		if(rightId == null)
			return;
		
		//Document right = RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
		DocumentWordCounts right = GlobalData.getInstance().id2wc.get(rightId);
		updateNearest(right, word2idf);
	}

	public boolean isNearestDetermined() {
		return GlobalData.getId2nearestOk(id);
	}

	public void setNearestDetermined(boolean nearestDetermined) {
		GlobalData.setId2nearestOk(id,  nearestDetermined);
	}

	public double getNearestDist() {
		return GlobalData.getId2nearestDist(id);
	}

	private void setNearestDist(double d) {
		GlobalData.setId2nearestDist(id, d);
	}

	public String getNearestId() {
		return GlobalData.getId2nearestId(id);
	} 
	
	public void setNearestId(String n) {
		GlobalData.setId2nearestId(id, n);
	} 
	
	//**************************************************************
	public static Document parse(String json, boolean isBasicOnly)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		
		if(jsonObj.get("text") == null || jsonObj.get("id_str") == null)
			return null;
		
		String text = jsonObj.get("text").getAsString();
		String id = jsonObj.get("id_str").getAsString();
		
		String created_at = jsonObj.get("created_at").getAsString();
		JsonElement element = jsonObj.get("timestamp");
		long timestamp;
		if(element != null)
			timestamp = element.getAsLong();
		else {
			//convert from created_at to timestamp
			timestamp = 0;
		}
			
		//id == "94816822100099073" is for Amy Winhouse event
		Document doc = new Document(id);
		doc.init(text, timestamp);
		doc.dirtyOn();
		
        doc.created_at = created_at;
        JsonObject userObj = jsonObj.get("user").getAsJsonObject();
    	doc.user_id = userObj.get("id_str").getAsString();			
		
        if(!isBasicOnly)
		{
        	//String retweeted_status = jsonObj.get("retweeted_status").getAsString();
			
        	element = jsonObj.get("in_reply_to_status_id_str");
			if(!element.isJsonNull())
				doc.reply_to = element.getAsString();
			
        	element = jsonObj.get("in_reply_to_user_id");
			if(!element.isJsonNull())
				doc.reply_to_user_id = element.getAsString();
			
        	element = jsonObj.get("quoted_status_id");
			if(element != null && !element.isJsonNull())
				doc.quoted_status_id = element.getAsString();
	        
			element = jsonObj.get("quoted_status");
			if(element!=null && !element.isJsonNull())
			{				
				JsonObject obj = element.getAsJsonObject();
				userObj = obj.get("user").getAsJsonObject();
	        	doc.quoted_user_id = userObj.get("id_str").getAsString();
			}
			
			doc.retweet_count = jsonObj.get("retweet_count").getAsInt();
			doc.favouritesCount = jsonObj.get("favorite_count").getAsInt();
	        
			element = jsonObj.get("retweeted_status");
			if(element!=null && !element.isJsonNull())
			{
				JsonObject retweetObj = element.getAsJsonObject();
				doc.retweeted_id = retweetObj.get("id_str").getAsString();
				
				userObj = retweetObj.get("user").getAsJsonObject();
	        	doc.retweeted_user_id = userObj.get("id_str").getAsString();
			}
			
			
		}        
        return doc;
	}
	
	public static Document parse01(String json, boolean isBasicOnly)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		
		JsonObject object = (JsonObject) jsonObj.get("object");
		
		if(object.get("summary") == null || object.get("id") == null)
			return null;
		
		String text = object.get("summary").getAsString();
		String id = object.get("id").getAsString();
		
		String created_at = jsonObj.get("postedTime").getAsString();
		JsonElement element = jsonObj.get("timestamp");
		long timestamp;
		if(element != null)
			timestamp = element.getAsLong();
		else {
			//convert from created_at to timestamp
			timestamp = 0;
		}
			
		//id == "94816822100099073" is for Amy Winhouse event
		Document doc = new Document(id);
		doc.init(text, timestamp);
		doc.dirtyOn();
		
        doc.created_at = created_at;
        JsonObject userObj = jsonObj.get("actor").getAsJsonObject();
    	doc.user_id = userObj.get("id").getAsString();	
    	 return doc;
		
	}
	
	public String getCreatedAt() {
		return created_at;
	}

	public String getRetweetedId() {
		return retweeted_id;
	}

	public int getRetweetCount() {
		return retweet_count;
	}

	public String getReplyTo() {
		return reply_to;
	}

	public String getUserId() {
		return user_id;
	}

	public int getFavouritesCount() {
		return favouritesCount;
	}

	public String getRetweetedUserId() {
		return retweeted_user_id;
	}

	public String getQuotedStatusId() {
		return quoted_status_id;
	}

	public String getQuotedUserId() {
		return quoted_user_id;
	}
	
	public String getReplyToUserId() {
		return reply_to_user_id;
	}

	public boolean isDirty() {
		return isDirtyBit;
	}

	public void dirtyOff() {
		isDirtyBit = false;
	}

	public void dirtyOn() {
		isDirtyBit = true;
	}

	public void setUserId(String asString) {
		user_id = asString;
		dirtyOn();
	}


}
