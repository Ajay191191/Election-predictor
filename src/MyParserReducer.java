import java.io.IOException;


import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;



public class MyParserReducer extends Reducer<Text, CompositeValueFormat, Text, Text>{

	
	
//    private Text outputKey = new Text();
//   
//     
//
//
//    public static String constructPropertyXml(Text name, Text value) {
//      StringBuilder sb = new StringBuilder();
//      sb.append("<Tweet><name>").append(name)
//          .append("</name><Text>").append(value)
//          .append("</Text></Tweet>");
//      return sb.toString();
//    }
//	
//	
//	
//	@Override
//	public void reduce(Text arg0, Iterator<Text> arg1,
//			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
//		 while(arg1.hasNext()) {
//		        outputKey.set(constructPropertyXml(arg0, arg1.next()));
//		        arg2.collect(outputKey, null);
//		
//		 }
//	}
	
//	@Override
//	public void reduce(Text key,Iterable<Text> value,Context context)
//		      throws IOException {
//		try {
//			for(Text val : value)
//			context.write(new Text("text"), new Text(val));
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

//	@Override
//	public void reduce(Text key, Iterator<Text> values,
//			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//		// TODO Auto-generated method stub
//		while(values.hasNext())
//		output.collect(new Text("text"), new Text(values.next().toString()));
//	}
	
	@Override
    protected void setup(
        Context context)
        throws IOException, InterruptedException {
      context.write(new Text("<?xml version=\"1.0\" encoding=\"UTF-8\"?> <All>"), null);
    }

    @Override
    protected void cleanup(
        Context context)
        throws IOException, InterruptedException {
      context.write(new Text("</All>"), null);
    }

    private Text outputKey = new Text();
    public void reduce(Text key, Iterable<CompositeValueFormat> values,
                       Context context)
        throws IOException, InterruptedException {
      for (CompositeValueFormat value : values) {
        outputKey.set(constructPropertyXml(key, value));
        context.write(outputKey, null);
      }
    }

    public static String constructPropertyXml(Text name, CompositeValueFormat value) {
      StringBuilder sb = new StringBuilder();
      sb.append("<Tweet><name>").append(name)
          .append("</name><Text>").append(value.getTweet())
          .append("</Text><Sentiment>").append(value.getSentiment())
          .append("</Sentiment><Certainty>").append(value.getCertainty())
          .append("</Certainty></Tweet>");
      return sb.toString();
    }
	

}
