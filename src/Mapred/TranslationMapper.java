package Mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.collections.map.HashedMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * 
 * @author root
 */
public class TranslationMapper extends
		Mapper<LongWritable, Text, Text, CompositeValueFormatTranslate> {

	private static final Logger log = LoggerFactory
			.getLogger(MyParserMapper.class);

	List<String> parties, people, hashTags;
	String matches[];
	
	NodeList nl;
	boolean found;
	XPath xPath;
	InputSource dDoc;
	boolean translated;
	BufferedReader in;
	String translatedText, inputLine;
	URL translate;
	String jsonText;
	List<String> TranslatedText;

	
	public enum UpdateCounter {
		  UPDATED
		  }

	@Override
	protected void setup(Context context) {
		found = false;
		xPath = XPathFactory.newInstance().newXPath();
	
		TranslatedText = new ArrayList<String>();
		matches = context.getConfiguration().get("xmlToSearch").toString().toLowerCase().split("\n");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String propertyScreenName;
		String propertyText;
		context.getCounter(UpdateCounter.UPDATED).increment(1);
		String oldDocument[] = value.toString().split("\n");
		String document = new String();
		document += "<Custom>";
		for(String s:oldDocument){
			if(!s.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>") && !s.equals("<Custom></Custom>")){
//				System.out.println("Line: " + s);
				document += s;
			}
		}
		propertyText = new String();
		propertyScreenName = new String();
		//System.out.println("Text: " + document);
		try {
			CompositeValueFormatTranslate cvf = new CompositeValueFormatTranslate();
			dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("//o/text", dDoc,
					XPathConstants.NODESET);
			for (int i = 0; i < nl.getLength(); i++) {
				propertyText += ("\""+nl.item(i).getTextContent()
						.replaceAll("\"|\\r|\\n|[^\\x00-\\x7F]", "")+"\"") +"\n";
			}
			dDoc = new InputSource(new StringReader(document));
			nl = (NodeList) xPath.evaluate("//o/user/screen_name", dDoc,
					XPathConstants.NODESET);
			for (int i = 0; i < nl.getLength(); i++) {
				propertyScreenName += ("\""+ nl.item(i).getTextContent()+"\"") +"\n";
			}

			cvf.setScreenName(propertyScreenName);
			cvf.setTweet(propertyText);
//			System.out.println(cvf.getTweet());
			cvf.setID((int)context.getCounter(UpdateCounter.UPDATED).getValue());
			context.write(new Text(context.getCounter(UpdateCounter.UPDATED).getValue()+" Map"), cvf);
		} catch (Exception e) {
			log.error("Error processing '" + document + "'", e);
		}
	}
}