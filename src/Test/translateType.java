package Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class translateType {

	public static void main(String[] args) throws IOException {
		System.out.println("ASd");
		BufferedReader facetInput = new BufferedReader(new FileReader("Facets"));
		String line = facetInput.readLine();
		int num = 1;
		String search = new String();
		String parties = new String();
		boolean partyEnd = true;
		String people = new String();
		boolean peopleEnd = true;
		String hashTags = new String();
		boolean hashtagEnd = true;
		while (line != null) {
			if(line.equals("Parties")){
				partyEnd = false;
				line = facetInput.readLine();
				continue;
			}
			if(line.equals("People")){
				peopleEnd = false;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if(line.equals("Hashtag")){
				hashtagEnd = false;
				peopleEnd = true;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if(!partyEnd){
				parties += line+"\n";
			}
			else if(!peopleEnd){
				people += line+"\n";
			}
			else if(!hashtagEnd){
				hashTags += line+"\n";
			}
			// System.out.println(line + " Trimmed:"+ line.replaceAll(" ", ""));
			search += line + "\n";
			line = facetInput.readLine();
		}
//		runJob(args[0], args[1], args[2], args[3], search,parties,people,hashTags);
		System.out.println("Parties : " +parties+"People: " +people+"Hashtags: " +hashTags + "Search: " +search);
	}
}
