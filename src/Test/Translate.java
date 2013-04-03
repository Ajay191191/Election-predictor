package Test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.python.core.PyObject;

public class Translate {

	

	public static void main(String[] args) throws Exception {
		String ur = URLEncoder
				.encode("RT @GliIntoccabili: #ariachetira #la7 @SenatoreMonti �quella frase del presidente #Berlusconi inizia con 'io prometto' quindi inutile as ...")
				.replaceAll("%23|%3F|%2F|\"", "");
		
		System.out.println(ur);
		 URL translate = new URL("http",
		 "localhost","/google_translate.php?text=" + ur);
		 URLConnection yc = translate.openConnection();
		 BufferedReader in = new BufferedReader(new InputStreamReader(
		 yc.getInputStream()));
		 String translatedText, inputLine;
		 translatedText = new String();
		 while ((inputLine = in.readLine()) != null) {
		 translatedText += inputLine;
		 }
		 System.out.println(translatedText);
		
		 String urlString = URLEncoder
		 .encode("This is a url http://url.com/?asd=asdasdasd%23");
		 System.out.println(urlString
		 + " Cut "
		 + urlString.replace("%23", "").replaceAll("http%3A%2F%2F[^ ]+",
		 ""));
		  //String s = "http://www.google.com?q=a b";
		  String s ="http://localhost:8604/v1/sentence/"+translatedText.trim()+".json";
		
		//  URI uri = new URI(parts[0], parts[1]+parts[2], null);
		 URL url = new URL("http", "localhost", 8604, "/v1/sentence/"
		  + URLEncoder.encode(translatedText) + ".json");
		 System.out.println(url.toURI().toString());
		 JSONObject json = readJsonFromUrl(url.toURI().toString());
		 System.out.println(json);
		 in.close();
		// String urlParameters = "language=english&text=" + translatedText;
		// String request = "http://text-processing.com/demo/sentiment/";
		// String reply = new String();
		// URL url1 = new URL(request);
		// URLConnection conn = url1.openConnection();
		//
		// conn.setDoOutput(true);
		//
		// OutputStreamWriter writer = new OutputStreamWriter(
		// conn.getOutputStream());
		//
		// writer.write(urlParameters);
		// writer.flush();
		//
		// String line, senti = new String();
		// // float neutral, polar, pos, neg;
		// float sentiment = 0;
		// BufferedReader reader = new BufferedReader(new InputStreamReader(
		// conn.getInputStream()));
		// int count = 0;
		// while ((line = reader.readLine()) != null) {
		// count++;
		// if (count == 15) {
		// if (line.contains("positive"))
		// senti = "positive";
		// else if(line.contains("negative"))
		// senti = "negative";
		// else
		// senti = "neutral";
		// }
		// // System.out.println(senti);
		// // if (count == 19)
		// // neutral = Float.parseFloat(line.split(":")[1]);
		//
		// // if (count == 22)
		// // polar = Float.parseFloat(line.split(":")[1]);
		//
		// if (count == 27 && senti.equals("positive"))
		// sentiment = Float.parseFloat(line.split(":")[1]) * 5;
		// // System.out.println(line);
		//
		// if (count == 28 && senti.equals("negative"))
		// // sentiment = (-1) * Float.parseFloat(line.split(":")[1]) * 5;
		// System.out.println(line);
		// }
		// // while ((line = reader.readLine()) != null) {
		// // reply += line;
		// // // System.out.println(line);
		// // // if(line.contains("large"))
		// // // System.out.println(line);
		// // }
		// //
		// PythonInterpreter interpreter = new PythonInterpreter();
		// interpreter.exec("from translate import translate");
		// translateClass=interpreter.get("translate");
		// //writer.close();
		// PyObject translateObject =translateClass.__call__(new PyString(ur));
		// translateType
		// t=(translateType)translateObject.__tojava__(translateType.class);
		// String sentiments=t.getResult();
		// System.out.println(sentiments);
		// // reader.close();

		// Document doc = Jsoup.parse(reply);

		// Elements el = doc.getElementsByClass("span-9");
		// for(Element e : el){
		// // System.out.println(e.getElementsByClass("positive").text());
		// // System.out.println(e.getElementsByClass("negative").text());
		//
		// String sp[]= e.getElementsByClass("positive").text().split(" ");
		// if(sp.length==1)
		// continue;
		// if(sp.length != 2){
		// sentiment =
		// Float.parseFloat(e.getElementsByClass("positive").text().split(":")[1].trim())
		// * 5;
		// }else{
		// sentiment = (-1) *
		// Float.parseFloat(e.getElementsByClass("negative").text().split(":")[1].trim())
		// * 5;
		// }
		//
		// }

		// TagNode rootNode;
		// HtmlCleaner html_cleaner = new HtmlCleaner();
		// rootNode = html_cleaner.clean(reader);
		// Object[] myNodes =
		// rootNode.evaluateXPath("//div[@class='span-9 last']//li[@class='positive']");
		// sentiment =
		// Float.parseFloat(((TagNode)myNodes[0]).getText().toString().split(":")[1]);
		//
		// System.out.println(sentiment + "");
		// float finalSentiment;
		// float jsonSentiment = Float
		// .parseFloat(json.get("sentiment").toString());
		// if (jsonSentiment == 0) {
		// finalSentiment = sentiment;
		// } else if (sentiment == 0) {
		// finalSentiment = jsonSentiment;
		// } else if ((jsonSentiment - sentiment) <= 5) {
		// finalSentiment = (sentiment + jsonSentiment) / 2;
		// } else {
		// finalSentiment = (sentiment + jsonSentiment);
		// }

		// System.out.println("Final Sentiment = " + finalSentiment);
		//ur="this is good";
		List<String> l = new ArrayList<String>();
		l.add("python");
		l.add("/home/hduser/twitter/translate.py");
		l.add("\"" + translatedText +"\"");
		System.out.println(l);
		ProcessBuilder b = new ProcessBuilder(l);
		Process p = b.start();
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String s1 ;
		// read the output

		//while ((s1 = stdInput.readLine()) != null) {
			s1 = stdInput.readLine();
			System.out.println(s1);
			String arr[]=s1.split(":");
			String arr1[] = arr[arr.length-1].split("}");
			System.out.println(arr1[0]);
		//}

	}

	public static JSONObject readJsonFromUrl(String url) throws IOException,
			JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is,
					Charset.forName("UTF-8")));
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
