package Mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class CompositeValueFormat implements Writable {
			private Text Tweet;
			private Text Sentiment;
			private float certainty;
			private Text Concerning;
			
			
			public CompositeValueFormat(){
				this.Tweet = new Text("");
				this.Sentiment = new Text("");
				this.certainty = 0.0f;
				this.Concerning = new Text("");
			}
			
		
			
			public Text getTweet()
			{
				return this.Tweet;
			}
			public void setTweet(Text tw){
				this.Tweet = tw;
			}
			
			public Text getConcerning()
			{
				return this.Concerning;
			}
			
			public void setConcerning(Text tw){
				this.Concerning= tw;
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
				this.Concerning = new Text(in.readUTF());
				
			}
			@Override
			public void write(DataOutput out) throws IOException {
				out.writeUTF(this.Tweet.toString());
				out.writeUTF(this.Sentiment.toString());
				out.writeFloat(this.certainty);
				out.writeUTF(this.Concerning.toString());
			}
}
