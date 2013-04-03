package Mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class SingleReducer extends
		Reducer<Text, CompositeValueFormatCombine, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?> <All>"), null);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(new Text("</All>"), null);
	}

	private Text outputKey = new Text();

	public void reduce(Text key, Iterable<CompositeValueFormatCombine> values,
			Context context) throws IOException, InterruptedException {
		for (CompositeValueFormatCombine value : values) {
			if (!value.getTweet().toString().equals("")) {
				outputKey.set(constructPropertyXml(key, value));
				context.write(outputKey, null);
			}
		}
	}

	public static String constructPropertyXml(Text name,
			CompositeValueFormatCombine value) {
		StringBuilder sb = new StringBuilder();
		sb.append("<Tweet><name>").append(name)
				.append("</name><Text Parties=\"").append(value.getParties())
				.append("\" People=\"").append(value.getPeople())
				.append("\" HashTags=\"").append(value.getHashTags()).append("\">")
				.append(value.getTweet()).append("</Text><Sentiment>")
				.append(value.getSentiment()).append("</Sentiment><Certainty>")
				.append(value.getCertainty()).append("</Certainty></Tweet>");
		return sb.toString();
	}

}
