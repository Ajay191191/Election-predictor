package Mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class CompositeValueFormat implements Writable {
			private Text Tweet;
			private float Sentiment;
			private float certainty;
			private Text Concerning;
			
			
			public CompositeValueFormat(){
				this.Tweet = new Text();
				this.Sentiment = 0;
				this.certainty = 0.0f;
				this.Concerning = new Text();
			}
			
		
			public float getSentiment()
			{
				return this.Sentiment;
			}
			
			public Text getTweet()
			{
				return this.Tweet;
			}
			public void setTweet(Text tw){
				this.Tweet = tw;
			}
			
			public void setSentiment(float s){
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
				this.Sentiment = in.readFloat();
				this.certainty = in.readFloat();
				
			}
			@Override
			public void write(DataOutput out) throws IOException {
				out.writeUTF(this.Tweet.toString());
				out.writeFloat(this.Sentiment);
				out.writeFloat(this.certainty);
				
			}
}
