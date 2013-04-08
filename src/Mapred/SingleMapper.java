package Mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import Mapred.TranslationMapper.UpdateCounter;

/**
 * 
 * @author root
 */
public class SingleMapper extends
		Mapper<LongWritable, Text, Text, CompositeValueFormatCombine> {

	private static final Logger log = LoggerFactory
			.getLogger(MyParserMapper.class);

	List<String> parties, people, hashTags;
	String matches[];
	String propertyScreenName = "";
	String propertyText = "";
	NodeList nl;
	boolean found;
	XPath xPath;
	InputSource dDoc;
	boolean translated;
	BufferedReader in;
	String translatedText, inputLine;
	URL translate;
	String jsonText;

	@Override
	protected void setup(Context context) {
		parties = new ArrayList<String>(Arrays.asList(context
				.getConfiguration().get("parties").toString().toLowerCase().split("\n")));
		people = new ArrayList<String>(Arrays.asList(context.getConfiguration()
				.get("people").toString().toLowerCase().split("\n")));
		hashTags = new ArrayList<String>(Arrays.asList(context
				.getConfiguration().get("hashTags").toString().split("\n")));
		matches = context.getConfiguration().get("xmlToSearch").toString().split("\n");
		found = false;
		xPath = XPathFactory.newInstance().newXPath();

	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String document = value.toString();
		try {
			CompositeValueFormatCombine cvf = new CompositeValueFormatCombine();
			dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("o/text", dDoc,
					XPathConstants.NODESET);
			propertyText = nl.item(0).getTextContent()
					.replaceAll("\"|\\r|\\n|[^\\x00-\\x7F]", "");
			dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("o/user/screen_name", dDoc,
					XPathConstants.NODESET);
			propertyScreenName = nl.item(0).getTextContent();
			for (int i = 0; i < matches.length; i++) {
				boolean Partyadded = false;
				boolean Peopleadded = false;
				boolean HashTagadded = false;
				String spl[] = matches[i].split(" ");
				String match = matches[i].replaceAll(" ", "");
				for (int j = 0; j < spl.length; j++)
					if (propertyText.toLowerCase().contains(spl[j].toLowerCase())) {
						found = true;
					}
				if (propertyText.toLowerCase().contains(match))
					found = true;
				if (found) {
					for (int j = 0; j < spl.length; j++) {
						if (parties.contains(spl[j].toLowerCase()) && !Partyadded) {
							cvf.setParties(new Text(matches[i]));
							Partyadded = true;
						}
						if (people.contains(spl[j].toLowerCase()) && !Peopleadded) {
							cvf.setPeople(new Text(matches[i]));
							Peopleadded = true;
						}
						if (hashTags.contains(spl[j].toLowerCase()) && !HashTagadded) {
							cvf.setHashTags(new Text(matches[i]));
							HashTagadded = true;
						}
					}
				}
			}
			if (found) {
				translated = false;
				in = null;

				translatedText = new String();
				int countG = 0;
				translate = new URL("http://localhost/google_translate.php?text="
						+ URLEncoder.encode(propertyText));
				while (countG <= 10) {
					URLConnection yc = translate.openConnection();
					in = new BufferedReader(new InputStreamReader(
							yc.getInputStream()));
					while ((inputLine = in.readLine()) != null) {
						translated = true;
						countG++;
						translatedText += inputLine;
					}
					if (translated)
						break;
				}
				in.close();

				// DefaultHttpClient httpclient = new DefaultHttpClient();
				// httpclient
				// .setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
				//
				// public long getKeepAliveDuration(
				// HttpResponse response, HttpContext context) {
				// // Honor 'keep-alive' header
				// HeaderElementIterator it = new BasicHeaderElementIterator(
				// response.headerIterator(HTTP.CONN_KEEP_ALIVE));
				// while (it.hasNext()) {
				// HeaderElement he = it.nextElement();
				// String param = he.getName();
				// String value = he.getValue();
				// if (value != null
				// && param.equalsIgnoreCase("timeout")) {
				// try {
				// return Long.parseLong(value) * 1000;
				// } catch (NumberFormatException ignore) {
				// }
				// }
				// }
				// HttpHost target = (HttpHost) context
				// .getAttribute(ExecutionContext.HTTP_TARGET_HOST);
				// return 30 * 1000 * 1000;
				// }
				//
				// });
				// URIBuilder builder = new URIBuilder();
				// builder.setScheme("http").setHost("localhost")
				// .setPath("/google_translate.php")
				// .setParameter("text", "Ciao");
				// URI uri = builder.build();
				// HttpGet httpget = new HttpGet(uri);
				// // System.out.println(httpget.getURI());
				//
				// HttpResponse response = httpclient.execute(httpget);
				// HttpEntity entity = response.getEntity();
				// if (entity != null) {
				// long len = entity.getContentLength();
				// if (len != -1 && len < 2048) {
				// translatedText = EntityUtils.toString(entity);
				// } else {
				// // Stream content out
				// }
				// }

				translate = new URL("http://master:8604/v1/sentence/"
						+ URLEncoder.encode(translatedText)
								.replaceAll("%23|%3F|%2F", "")
								.replaceAll("http%3A%2F%2F[^ ]+", "") + ".json");
				jsonText = new String();
				URLConnection yc = translate.openConnection();
				in = new BufferedReader(new InputStreamReader(
						yc.getInputStream()));
				while ((inputLine = in.readLine()) != null)
					jsonText += inputLine;

				JSONObject json = new JSONObject(jsonText);

				List<String> l = new ArrayList<String>();
				l.add("python");
				l.add("/twitter/translate.py");
				l.add("\"" + translatedText.replaceAll("[^\\x00-\\x7F]", "")
						+ "\"");
				String s;
				String secondsentiment = new String();
				ProcessBuilder b = new ProcessBuilder(l);
				int count = 0;
				while (count <= 10) {
					Process p = b.start();
					BufferedReader stdInput = new BufferedReader(
							new InputStreamReader(p.getInputStream()));
					BufferedReader stdError = new BufferedReader(
							new InputStreamReader(p.getErrorStream()));
					s = stdInput.readLine();
					if (s == null) {
						System.out.println("Failed : "
								+ translatedText.replaceAll("[^\\x00-\\x7F]",
										"") + "Original:" + propertyText);
						count++;
						String s2;
						while ((s2 = stdError.readLine()) != null)
							System.out.println(s2);
						continue;
					}
					String arr[] = s.split(":");
					String arr1[] = arr[arr.length - 1].split("}");
					secondsentiment = arr1[0];
					break;
				}
				String finalSentiment = new String();
				float jsonSentiment = Float.parseFloat(json.get("sentiment")
						.toString());
				// float jsonSentiment = 4 ;
				if (secondsentiment.trim().equals("\'neutral\'"))
					if (jsonSentiment >= 0)
						finalSentiment = new String("positive");
					else if (jsonSentiment <= 0)
						finalSentiment = new String("negative");
					else
						finalSentiment = new String("neutral");
				if (secondsentiment.trim().equals("\'positive\'"))
					finalSentiment = new String("positive");
				if (secondsentiment.trim().equals("\'negative\'"))
					finalSentiment = new String("negative");
				cvf.setTweet(new Text(propertyText));
				cvf.setSentiment(new Text(finalSentiment));
				cvf.setCertainty(Float.parseFloat(json.get("certainty")
						.toString()));
				context.write(new Text(propertyScreenName.trim()), cvf);
			} else
				context.write(new Text(""), new CompositeValueFormatCombine());

		} catch (Exception e) {
			log.error("Error processing '" + document + "'", e);
		}
	}
}