package Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathExpressionException;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.json.JSONException;
import org.json.JSONObject;

public class translateType {

	public static void main(String[] args) throws IOException,
			URISyntaxException, XPathExpressionException, JSONException {
//		// BufferedReader facetInput = new BufferedReader(new
//		// FileReader("/home/ajay/Project/erged.xml"));
//		// String line = facetInput.readLine();
//		// List<String> tweets1 = new ArrayList<String>();
//		// while((line!=null)){
//		// tweets1.add(line);
//		// }
////		String list = "\"BERSANI anzich pensare alle elezioni ..... stava formando il governo !?!?!?!\"| \"Oggi pi che mai ho sbarrato il simbolo #ladestra con orgoglio! #elezioni #storace #razza\"| \"RT @frandiben: A elezioni ultimate Berlusconi indagato per compravendita di senatori| Gentile da parte dei giudici rispettare il silenzi ...\"| \"#M5S voter in aula le leggi che rispecchiano il suo programma chiunque sia a proporle| (Beppe Grillo 27.02 ore 14.17) #elezioni #crisi\"| \"Elezioni 2013. Berlusconi contestato da attiviste di Femen in topless : ENNESIMA FIGURA DA PAGLIACCIO\"| \"RT @SandroVeronesi: D'Alema e Berlusconi dicono la stessa cosa su ipotesi nuove elezioni: niet. Ora  veramente facilissimo capire qual' ...\"| \"Foto: aerre2: #sveglia #italia #elezioni #ElectionDay #Italy  http://t.co/54fltDwCNE\"| \"Elezioni 2013: Movimento 5 stelle primo partito d'Italia il Pd apre le porte a Grillo http://t.co/gdXEc5Inmy\"| \"RT @giucruciani: ++++++ ANSA: ELEZIONI. GRILLO. CUSTODE VILLA: STA PRANZANDO ++++++\"| \"Un governo B/B sarebbe un abbraccio mortale per Bersan.una vittoria ineluttabile di Berlusconi alle prossime elezioni e un calo 5S.Lo vuoi?\""
////				.replaceAll("[^a-zA-Z |]", "");
//		String list = "RT beppegrillo Se Bersani vorr proporre labolizione dei contributi pubblici ai partiti sin dalle ultime elezioni lo voteremo di slancio|Elezioni Bersani sfida Grillo  httptcodIjjkYbzUg via repubblicait|RT mortozombie Berlusconi restituir barbara durso alla tangenziale sapevatelo elezioni domenicalive|Elezioni  attiviste Femen contestano Berlusconi  Repubblicait httptcotShawLLaS via repubblicait|RT OltreMedia Elezioni Berlusconi vota femministe si spogliano caos httptcoDKKZBEre|RT LazioN ANZIO IL GIORNO DELLE ELEZIONI MACCHIATO DA UN TENTATIVO DI BROGLIO FERMATA MILITANTE DI SINISTRA Storace | httptc |Italia e mondo Italy and World  Italia y Mundo Elezioni regionali dati considerazioni httptcoIdEKwofa|RT Vogliadarianuov Elezioni  Berlusconi  stato il primo ad assistere allo spoglio  MHPN|RT Matteo AndreaScanzi Grillo Dalle prossime elezioni le matite copiative saranno tassativamente al gusto di menta elezioni|RT beppegrillo Se Bersani vorr proporre labolizione dei contributi pubblici ai partiti sin dalle ultime elezioni lo voteremo di slancio|".replaceAll("[^a-zA-Z |]", "");
//		//
//		List<String> tweets = new ArrayList<String>(Arrays.asList(list
//				.split("\n")));
//		
//		
		DefaultHttpClient httpclient = new DefaultHttpClient();
		httpclient.setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
			public long getKeepAliveDuration(HttpResponse response,
					HttpContext context) {
				// Honor 'keep-alive' header
				HeaderElementIterator it = new BasicHeaderElementIterator(
						response.headerIterator(HTTP.CONN_KEEP_ALIVE));
				while (it.hasNext()) {
					HeaderElement he = it.nextElement();
					String param = he.getName();
					String value = he.getValue();
					if (value != null && param.equalsIgnoreCase("timeout")) {
						try {
							return Long.parseLong(value) * 1000;
						} catch (NumberFormatException ignore) {
						}
					}
				}
				HttpHost target = (HttpHost) context
						.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
				if ("www.naughty-server.com".equalsIgnoreCase(target
						.getHostName())) {
					// Keep alive for 5 seconds only
					return 5 * 1000;
				} else {
					// otherwise keep alive for 30 seconds
					return 30 * 1000 * 1000;
				}
			}

		});
//		HttpPost httppost = new HttpPost(
//				"http://localhost/google_translate1.php");
//
//		List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(1);
//		nameValuePairs.add(new BasicNameValuePair("text", tweets.toString()
//				.replaceAll("[^a-zA-Z |]", "")));
//		httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
//		System.out.println(nameValuePairs);
//		List<String> as = new ArrayList<String>();
//
//		HttpResponse response = httpclient.execute(httppost);
//		// HttpEntity entity = response.getEntity();
//		// if (entity != null) {
//		// long len = entity.getContentLength();
//		// if (len != -1) {
//		// String repl = EntityUtils.toString(entity);
//		// System.out.println(repl);
//		// String rep = URLDecoder.decode(repl);
//		// // System.out.println(URLDecoder.decode(rep.replaceAll(" ","")));
//		// Pattern p = Pattern.compile("\\[(.*?)\\]");
//		// Matcher m = p.matcher(rep);
//		//
//		// while(m.find()) {
//		// rep=m.group(1);
//		// }
//		// String ind[] = rep.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//		// for(String s:ind)
//		// as.add(s.trim());
//		// //as = new ArrayList<String>(Arrays.asList());
//		// } else {
//		// // Stream content out
//		// }
//		// }
//		BufferedReader rd = new BufferedReader(new InputStreamReader(response
//				.getEntity().getContent()));
//		String line = "";
//		String lin = "";
//		while ((line = rd.readLine()) != null) {
//			// System.out.println(line);
//			lin += line.replaceAll("\\[+","[");
//			
//		}
////		System.out.println(lin);
//		
//		String rep = null;
//		Pattern p = Pattern.compile("\\[(.*?)\\]");
//		Matcher m = p.matcher(lin);
//		String translatedCombined = new String();
//		while (m.find()) {
//			
//			rep = m.group(1);
////			System.out.println(rep);
////			Pattern pm = Pattern.compile("\"([^\"]*)\"");
////			Matcher mm = p.matcher(rep);
////			while (mm.find()) {
////			  System.out.println(mm.group(1));
////			}
//			//System.out.println(rep);
//			
//			String ind[] = rep.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//			translatedCombined += ind[0].replaceAll("^\"|\"$", "");;
//		}
//		System.out.println(translatedCombined);
//////		String ind[] = rep.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//////		for (String s : ind)
////			as.add(s.trim());
//		
//		String seperatedText[] = translatedCombined.split("\\|");
//		List<String>Trans = new ArrayList<String>(Arrays.asList(translatedCombined.split("\\|")));
//		System.out.println("Tweets : " + tweets.size()  + " Trans  " + Trans.size());
////		for(String s: seperatedText){
////		URL translate = new URL(
////				"http://master:8604/v1/sentence/"
////						+ URLEncoder
////								.encode(s.trim())
////								.replaceAll("%23|%3F|%2F", "")
////								.replaceAll(
////										"http%3A%2F%2F[^ ]+",
////										"") + ".json");
////		String jsonText = new String();
////		URLConnection yc = translate.openConnection();
////		BufferedReader in = new BufferedReader(new InputStreamReader(
////				yc.getInputStream()));
////		String inputLine;
////		while ((inputLine = in.readLine()) != null)
////			jsonText += inputLine;
////
////		JSONObject json = new JSONObject(jsonText);
////		System.out.println(json);
////		}
//		
//		//
//		// BufferedReader facetInput = new BufferedReader(new
//		// FileReader("Custom_XML"));
//		// String lines = new String();
//		// String s = new String();
//		// while((lines=facetInput.readLine())!=null){
//		// if(!lines.equals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")){
//		// //lines.replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>","");
//		// System.out.println(lines);
//		// s += lines;
//		// }
//		//
//		// }
//		//
//		// //System.out.println(s);
//		//
//		//
//		// XPath xPath = XPathFactory.newInstance().newXPath();
//		//
//		//
//		//
//		// InputSource dDoc = new InputSource(new StringReader(s));
//		// NodeList nl = (NodeList) xPath.evaluate("//o/text",
//		// dDoc,XPathConstants.NODESET);
//		// for (int i = 0; i < nl.getLength(); i++) {
//		// System.out.println(nl.item(i).getTextContent()+"");
//		// }
//		
		String locationCity = "italia,IT|Milano,IT|Antibes|Bolzano, Italia,IT|";
		HttpPost httppost = new HttpPost("http://localhost/getLatLong.php");
		List<NameValuePair> nameValuePairs1 = new ArrayList<NameValuePair>(
				1);
		nameValuePairs1.add(new BasicNameValuePair("address", locationCity.replaceAll("[^a-zA-Z,|]", "")));
		httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs1));
//		System.out.println(nameValuePairs1);
		HttpResponse response = httpclient.execute(httppost);
		BufferedReader rd = new BufferedReader(new InputStreamReader(
				response.getEntity().getContent()));
		String lines = new String();
		String location=new String();
		while ((lines = rd.readLine()) != null) {
//			 System.out.println(lines);
			location += lines;
		}
		
		Pattern p = Pattern.compile("\\[(.*?)\\]");
		Matcher m = p.matcher(location);
		while (m.find()) {
			System.out.println(m.group(1));
		}
			
		
		
	}
	public static void removeDuplicates(List<String> list) {
		HashSet<String> set = new HashSet<String>(list);
		list.clear();
		list.addAll(set);
	}
}

