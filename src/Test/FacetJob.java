package Test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class FacetJob {

	public static void main(String[] args) throws IOException {
		BufferedReader facetInput = new BufferedReader(new FileReader("Facets"));
		String line = facetInput.readLine();
        while (line != null) {
        		  //System.out.println(line + " Trimmed:"+ line.replaceAll(" ", ""));
        		  String matches = line + "\n"+ line.replaceAll(" ", "");
        		  String mat[] = matches.split("\n");
        		  for(int i=0;i<mat.length;i++){
        			  System.out.println(mat[i]);
        		  }
                  line = facetInput.readLine();
        }
	}
}
