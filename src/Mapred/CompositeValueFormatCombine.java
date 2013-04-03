package Mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class CompositeValueFormatCombine implements Writable {
			private Text Tweet;
			private Text Sentiment;
			private float certainty;
//			private Text Concerning;
			private Text parties;
			private Text people;
			private Text hashTags;
			
			public CompositeValueFormatCombine(){
				this.Tweet = new Text("");
				this.Sentiment = new Text("");
				this.certainty = 0.0f;
				this.parties = new Text("");
				this.people = new Text("");
				this.hashTags= new Text("");
			}
			
		
			
			public Text getTweet()
			{
				return this.Tweet;
			}
			public void setTweet(Text tw){
				this.Tweet = tw;
			}
			
			public Text getParties()
			{
				return this.parties;
			}
			
			public void setParties(Text tw){
				
				this.parties = new Text(new String(this.parties.toString() + tw + " ")); 
			}
			public Text getPeople()
			{
				return this.people;
			}
			
			public void setPeople(Text tw){
				
				this.people= new Text(new String(this.people.toString() + tw + " "));
			}
			public Text getHashTags()
			{
				return this.hashTags;
			}
			
			public void setHashTags(Text tw){
				this.hashTags= new Text(new String(this.hashTags.toString() + tw + " "));
			}

			

			public Text getSentiment()
			{
				return this.Sentiment;
			}
			public void setSentiment(Text s){
				this.Sentiment = s;
			}
			
			public void setCertainty(float certainty){
				this.certainty = certainty;
			}
			
			public float getCertainty(){
				return this.certainty;
			}
			
			@Override
			public void readFields(DataInput in) throws IOException {
				this.Tweet = new Text(in.readUTF());
				this.Sentiment = new Text(in.readUTF());
				this.certainty = in.readFloat();
				this.parties= new Text(in.readUTF());
				this.people = new Text(in.readUTF());
				this.hashTags= new Text(in.readUTF());
				
			}
			@Override
			public void write(DataOutput out) throws IOException {
				out.writeUTF(this.Tweet.toString());
				out.writeUTF(this.Sentiment.toString());
				out.writeFloat(this.certainty);
				out.writeUTF(this.parties.toString());
				out.writeUTF(this.people.toString());
				out.writeUTF(this.hashTags.toString());
				
			}
}
