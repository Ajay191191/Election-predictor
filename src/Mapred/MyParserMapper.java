package Mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * 
 * @author root
 */
public class MyParserMapper extends
		Mapper<LongWritable, Text, Text, CompositeValueFormat> {

	private static final Logger log = LoggerFactory
			.getLogger(MyParserMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String document = value.toString();
		try {

			XPath xPath = XPathFactory.newInstance().newXPath();
			;

			String propertyScreenName = "";
			String propertyText = "";
			NodeList nl;
			boolean found = false;
			CompositeValueFormat cvf = new CompositeValueFormat();
			InputSource dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("o/text", dDoc,
					XPathConstants.NODESET);
			propertyText = nl.item(0).getTextContent()
					.replaceAll("\"|\\r|\\n|[^\\x00-\\x7F]", "");
			dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("o/user/screen_name", dDoc,
					XPathConstants.NODESET);
			propertyScreenName = nl.item(0).getTextContent();

			String matches[] = context.getConfiguration().get("xmlToSearch")
					.toString().toLowerCase().split("\n");
			cvf.setConcerning(new Text(matches[0]));
			for (int i = 0; i < matches.length; i++) {
				String spl[] = matches[i].split(" ");
				for (int j = 0; j < spl.length; j++)
					if (propertyText.toLowerCase().contains(spl[j])) {
						found = true;
					}
			}
			if (found) {
				propertyText.replaceAll("", "");
				boolean translated = false;
				BufferedReader in = null;
				String translatedText, inputLine;
				translatedText = new String();
				int countG = 0;
				URL translate = new URL(
						"http://master/google_translate.php?text="
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
				JSONObject json = readJsonFromUrl("http://master:8604/v1/sentence/"
						+ URLEncoder.encode(translatedText)
								.replaceAll("%23|%3F|%2F", "")
								.replaceAll("http%3A%2F%2F[^ ]+", "") + ".json");
				List<String> l = new ArrayList<String>();
				l.add("python");
				l.add("/home/hduser/twitter/translate.py");
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
				context.write(new Text(""), new CompositeValueFormat());

		} catch (Exception e) {
			log.error("Error processing '" + document + "'", e);
		}
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