package Test;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



public class Translate {
	public static void main(String[] args) throws Exception {
		String ur = URLEncoder.encode("nullElections: # Tremonti, vote for us and 'message to Berlusconi.").replaceAll("%23|%3F|%2F", "");
		URL translate = new URL("http","localhost","/google_translate.php?text="+ur);
	      URLConnection yc = translate.openConnection();
	      BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
	      String translatedText ,inputLine;
	      translatedText = new String();
			 while ((inputLine = in.readLine()) != null) {
		          translatedText += inputLine;
			 }
			 System.out.println(translatedText);
			 
			 String urlString = URLEncoder.encode("This is a url http://url.com/?asd=asdasdasd%23");
			 System.out.println(urlString + " Cut " +urlString.replace("%23", "").replaceAll("http%3A%2F%2F[^ ]+", ""));
			 //String s = "http://www.google.com?q=a b";
			 //String s = "http://localhost:8604/v1/sentence/"+translatedText.trim()+".json";
			 
			 //URI uri = new URI(parts[0], parts[1]+parts[2], null);
	      URL url = new URL("http","localhost",8604,"/v1/sentence/"+URLEncoder.encode(translatedText)+".json");
	      System.out.println(url.toURI().toString());
	      JSONObject json = readJsonFromUrl(url.toURI().toString());
	      System.out.println(json);
	      in.close();
	      String urlParameters = "language=english&text="+ur;
	      String request = "http://text-processing.com/demo/sentiment/";
	      String reply = new String();
	      URL url1 = new URL(request);
	      URLConnection conn = url1.openConnection();

	      conn.setDoOutput(true);

	      OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());

	      writer.write(urlParameters);
	      writer.flush();
	      
	      String line;
	      BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

	      while ((line = reader.readLine()) != null) {
	    	  reply += line;
	        //  System.out.println(line);
//	    	  if(line.contains("large"))
//	    		  System.out.println(line);
	      }
	      
	      writer.close();
	      reader.close();     
	      
	      Document doc = Jsoup.parse(reply);
	      float sentiment = 0;
	      Elements el = doc.getElementsByClass("span-9");
	      for(Element e : el){
//	    	  System.out.println(e.getElementsByClass("positive").text());
//	    	  System.out.println(e.getElementsByClass("negative").text());

	    	  String sp[]= e.getElementsByClass("positive").text().split(" ");
	    	  if(sp.length==1)
	    		  continue;
	    	  if(sp.length != 2){
	    		  sentiment = Float.parseFloat(e.getElementsByClass("positive").text().split(":")[1].trim()) * 5;
	    	  }else{
	    		  sentiment = (-1) * Float.parseFloat(e.getElementsByClass("negative").text().split(":")[1].trim()) * 5;
	    	  }
	    	  
	      }
	      System.out.println(sentiment+"");
	      float finalSentiment;
	      float jsonSentiment = Float.parseFloat(json.get("sentiment").toString());
	      if(jsonSentiment==0){
	    	  finalSentiment = sentiment;
	      }
	      else if(sentiment == 0){
	    	  finalSentiment = jsonSentiment;
	      }
	      else if((jsonSentiment-sentiment) <= 5){
	    	  finalSentiment = (sentiment + jsonSentiment )/2;
	      }
	      else{
	    	  finalSentiment = (sentiment + jsonSentiment);
	      }
	      
	      System.out.println("Final Sentiment = " + finalSentiment);
	}
	  
	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
	    InputStream is = new URL(url).openStream();
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	      String jsonText = readAll(rd);
	      JSONObject json = new JSONObject(jsonText);
	      return json;
	    } finally {
	      is.close();
	    }
	  }
	
	private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }
	
}
