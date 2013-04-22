package Test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;


public class FacetJob {

	public static void main(String[] args) throws IOException {
		BufferedReader facetInput = new BufferedReader(new FileReader("Facets"));
		// System.out.println(args[0]);
		
		String line = facetInput.readLine();
		int num = 1;
		List<String> search = new ArrayList<String>();
		List<String> parties =new ArrayList<String>();
		boolean partyEnd = true;
		List<String> people = new ArrayList<String>();
		boolean peopleEnd = true;
		List<String> hashTags = new ArrayList<String>();
		boolean hashtagEnd = true;
		List<String> translations = new ArrayList<String>();
		boolean translationsEnd = true;
		while (line != null) {
			if (line.equals("Parties")) {
				partyEnd = false;
				line = facetInput.readLine();
				continue;
			}
			if (line.equals("People")) {
				peopleEnd = false;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if (line.equals("Hashtag")) {
				hashtagEnd = false;
				peopleEnd = true;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if(line.equals("Translations")){
				hashtagEnd = true;
				peopleEnd = true;
				partyEnd = true;
				translationsEnd = false;
				line = facetInput.readLine();
				continue;
			}
			if (!partyEnd) {
				parties.add(line + "\n");
			} else if (!peopleEnd) {
				people .add(line + "\n");
			} else if (!hashtagEnd) {
				hashTags .add( line + "\n");
			} else if(!translationsEnd){
				translations.add(line + "\n");
			}
			// System.out.println(line + " Trimmed:"+ line.replaceAll(" ", ""));
			search .add(line + "\n");
			line = facetInput.readLine();
		}
		
		System.out.println("Parties : " + parties);
		System.out.println("People : " + people);
		System.out.println("HashTags : " + hashTags);
		System.out.println("Translations : " + translations);
		
		String s;
		if((s=contains1(translations,"Movimento 5"))!=null){
			System.out.println(s);
		}
		
	}
	
	static boolean containsList(List<String> list, String word) {
		for (String s : list) {
				if(s.toLowerCase().trim().equals(word))
					return true;
			}
		return false;
	}
	
	static boolean contains(List<String> list, String word) {
		for (String s : list) {
			String splits[] = s.split(" ");
			for(String sp:splits){
				if(sp.equals(word))
					return true;
			}
		}
		return false;
	}
	
	static String contains1(List<String> list, String word) {
		for (String s : list) {
			if(s.toLowerCase().trim().contains(word.toLowerCase()))
				return s.split("-")[0];
		}
		return null;
	}
}
