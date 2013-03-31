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
public class MyParserMapper extends Mapper<LongWritable, Text, Text, CompositeValueFormat> {


	
	private static final Logger log = LoggerFactory.getLogger
		      (MyParserMapper.class);
//
//    @Override
//    public void map(LongWritable key, Text value1,Context context)
//
//throws IOException, InterruptedException {
//    	String xmlString = value1.toString();
//        
//        SAXBuilder builder = new SAXBuilder();
//       Reader in = new StringReader(xmlString);
//String value="";
//   try {
//       
//       Document doc = builder.build(in);
//       Element root = doc.getRootElement();
//       
//       String tag1 =root.getChild("tag").getChild("tag1").getTextTrim() ;
//        
//       String tag2 =root.getChild("tag").getChild("tag1").getChild("tag2").getTextTrim();
//        value= tag1+ ","+tag2;
//        context.write(new Text("text"), new Text(value));
//   } catch (JDOMException ex) {
//       Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
//   } catch (IOException ex) {
//       Logger.getLogger(MyParserMapper.class.getName()).log(Level.SEVERE, null, ex);
//   }
//
//
//              
//    
//    }

//	@Override
//	public void map(LongWritable key, Text value1,
//			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//		// TODO Auto-generated method stub
//		  String document = value1.toString();
//	      System.out.println("'" + document + "'");
//	      try {
//	        XMLStreamReader reader =
//	            XMLInputFactory.newInstance().createXMLStreamReader(new
//	                ByteArrayInputStream(document.getBytes()));
//	        String propertyName = "";
//	        String propertyValue = "";
//	        String currentElement = "";
//	        while (reader.hasNext()) {
//	          int code = reader.next();
//	          switch (code) {
//	            case START_ELEMENT:
//	              currentElement = reader.getLocalName();
//	              break;
//	            case CHARACTERS:
//	              if (currentElement.equalsIgnoreCase("screen_name")) {
//	                propertyName += reader.getText();
//	              } else if (currentElement.equalsIgnoreCase("text")) {
//	                propertyValue += reader.getText();
//	              }
//	              break;
//	          }
//	        }
//	        reader.close();
//	        output.collect(new Text(propertyName.trim()),new Text(propertyValue.trim()));
//	      } catch (Exception e) {
//	        log.error("Error processing '" + document + "'", e);
//	      }
//}
	
	@Override
    protected void map(LongWritable key, Text value,
                       Context context)
        throws
        IOException, InterruptedException {
		


		
      String document = value.toString();
      DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
     // System.out.println("'" + document + "'");
      try {
    	  
    	  //With XMLStreamReader
//        XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
        String propertyScreenName = "";
        String propertyText = "";
        NodeList nl;
//        String currentElement = "";
//        String previousElement = "o/"; 
//        while (reader.hasNext()) {
//          int code = reader.next();
//          switch (code) {
//            case START_ELEMENT:
//              currentElement = reader.getLocalName();
//              previousElement.concat(currentElement+"/");
//              previousElement += currentElement+"/";
//       //       System.out.println("CurrentElement: " + currentElement);
//        //      System.out.println("PreviousElement_Start: " + previousElement);
//              break;
//            case END_ELEMENT:
//         //   	System.out.println("PreviousElement: " + previousElement);
//            	previousElement.replaceAll(currentElement+"/", ""); 
//            	
//            	break;
//            case CHARACTERS:
//            	
//              if (currentElement.equalsIgnoreCase("screen_name") && previousElement.equalsIgnoreCase("o/user/")) {
//                propertyName += reader.getText();
//              } else if (currentElement.equalsIgnoreCase("text") && previousElement.equalsIgnoreCase("o/")) {
//                propertyValue += reader.getText();
//              }
//              break;
//          }
//        }
//        reader.close();
//        if(propertyValue.toLowerCase().contains(context.getConfiguration().get("xmlToSearch").toString().toLowerCase()))
//        	context.write(new Text(propertyName.trim()),new Text(propertyValue.trim()));
//        else
//        	context.write(new Text(propertyName.trim()),new Text(""));
    	  
    	  CompositeValueFormat cvf = new CompositeValueFormat();
    	  //WithXPath
          InputSource dDoc = new InputSource(new StringReader(document));
          XPath xPath = XPathFactory.newInstance().newXPath();
          nl = (NodeList) xPath.evaluate("o/text", dDoc, XPathConstants.NODESET);
          //System.out.println("Text : " + nl.item(0).getTextContent());
          propertyText = nl.item(0).getTextContent();
          dDoc = new InputSource(new StringReader(document));
          nl = (NodeList) xPath.evaluate("o/user/screen_name", dDoc, XPathConstants.NODESET);
          propertyScreenName = nl.item(0).getTextContent();
          
          
          
          String matches[] = context.getConfiguration().get("xmlToSearch").toString().toLowerCase().split("\n");
			for (int i = 0; i < matches.length; i++) {
				if (propertyText.toLowerCase().contains(matches[i])){
					URL translate = new URL("http://master/google_translate.php?text="+URLEncoder.encode(propertyText));
				      URLConnection yc = translate.openConnection();
				      BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
					String translatedText ,inputLine;
					translatedText=new String();
					 while ((inputLine = in.readLine()) != null) 
				          translatedText += inputLine;
				      in.close();
					//curl -X GET -g "http://localhost:8080/v1/sentence/fantastic.json
					 JSONObject json = readJsonFromUrl("http://localhost:8604/v1/sentence/"+URLEncoder.encode(translatedText).replaceAll("%23|%3F|%2F", "").replaceAll("http%3A%2F%2F[^ ]+", "")+".json");
					 
					 
					 
					cvf.setTweet(new Text(propertyText));
					 cvf.setSentiment(Float.parseFloat(json.get("sentiment").toString()));
					 cvf.setCertainty(Float.parseFloat(json.get("certainty").toString()));
					context.write(new Text(propertyScreenName.trim()),cvf);
				}
				else
					context.write(new Text(""), new CompositeValueFormat());
			}
      } catch (Exception e) {
        log.error("Error processing '" + document + "'", e);
      }
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