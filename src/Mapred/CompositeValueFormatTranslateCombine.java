package Mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class CompositeValueFormatTranslateCombine implements Writable{
	
	private List<String> TranslatedText;


	 
	public CompositeValueFormatTranslateCombine(){
		this.TranslatedText= new ArrayList<String>();
	
	}

	public List<String> getTranslatedText() {
		return this.TranslatedText;
	}

	public void setTranslatedText(List<String> tw) {
		this.TranslatedText= new ArrayList<String>(tw);
	}

	

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			this.TranslatedText.add(in.readUTF());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.TranslatedText.size());
		for (String element : this.TranslatedText) {
			out.writeUTF(element);
		}
	
		
	} 

}
