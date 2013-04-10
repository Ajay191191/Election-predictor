package Mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CompositeValueFormatTranslate implements Writable {
	private String Tweet;
	private String ScreenName;
	private String TranslatedText;
	private String Location;
	private int ID;

	public CompositeValueFormatTranslate() {
		this.Tweet = new String();
		this.ScreenName = new String();
		this.TranslatedText= new String();
		this.Location= new String();
		ID=0;
	}
	
	public String getTranslatedText() {
		return this.TranslatedText;
	}

	public void setTranslatedText(String tw) {
		this.TranslatedText= new String(tw);
	}
	
	public String getLocation() {
		return this.Location;
	}

	public void setLocation(String tw) {
		this.Location= new String(tw);
	}

	public int getID() {
		return this.ID;
	}

	public void setID(int tw) {
		this.ID = tw;
	}



	public String getTweet() {
		return this.Tweet;
	}

	public void setTweet(String tw) {
		this.Tweet = new String(tw);
	}

	public String getScreenName() {
		return this.ScreenName;
	}

	public void setScreenName(String tw) {
		this.ScreenName = new String(tw);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.Tweet = in.readUTF();
		this.ScreenName = in.readUTF();
		this.TranslatedText = in.readUTF();
		this.Location = in.readUTF();
		this.ID = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.Tweet);
		out.writeUTF(this.ScreenName);
		out.writeUTF(this.TranslatedText);
		out.writeUTF(this.Location);
		out.writeInt(ID);
	
	}
}
