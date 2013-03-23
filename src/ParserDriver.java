import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.slf4j.*;

/**
 * 
 * @author root
 */
public class ParserDriver {

	// /**
	// * @param args
	// * the command line arguments
	// */
	// public static void main(String[] args) {
	//
	// Configuration conf = new Configuration();
	//
	// conf.set("xmlinput.start", "<text>");
	// conf.set("xmlinput.end", "</text>");
	// conf.set(
	// "io.serializations",
	// "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	//
	// Job job;
	// try {
	// job = new Job(conf, "jobName");
	//
	// FileInputFormat.setInputPaths(job, args[0]);
	// job.setJarByClass(ParserDriver.class);
	// job.setMapperClass(MyParserMapper.class);
	// job.setReducerClass(MyParserReducer.class);
	// // job.setNumReduceTasks(0);
	// job.setInputFormatClass(XmlInputFormat.class);
	// job.setOutputKeyClass(NullWritable.class);
	// job.setOutputValueClass(Text.class);
	// Path outPath = new Path(args[1]);
	// FileOutputFormat.setOutputPath(job, outPath);
	// FileSystem dfs = null;
	//
	// dfs = FileSystem.get(outPath.toUri(), conf);
	// if (dfs.exists(outPath)) {
	// dfs.delete(outPath, true);
	// }
	// job.waitForCompletion(true);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	//
	// catch (InterruptedException ex) {
	// Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
	// null, ex);
	// } catch (ClassNotFoundException ex) {
	// Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
	// null, ex);
	// }
	//
	// }
	//
	// public static void runJob(String input, String output) throws IOException
	// {
	//
	// // Configuration conf = new Configuration();
	// //
	// // conf.set("xmlinput.start", "<text>");
	// // conf.set("xmlinput.end", "</text>");
	// // conf.set(
	// // "io.serializations",
	// //
	// "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	// //
	// // Job job = new Job(conf, "jobName");
	// //
	// // FileInputFormat.setInputPaths(job, input);
	// // job.setJarByClass(ParserDriver.class);
	// // job.setMapperClass(MyParserMapper.class);
	// // job.setReducerClass(MyParserReducer.class);
	// // //job.setNumReduceTasks(0);
	// // job.setInputFormatClass(XmlInputFormat.class);
	// // job.setOutputKeyClass(NullWritable.class);
	// // job.setOutputValueClass(Text.class);
	// // Path outPath = new Path(output);
	// // FileOutputFormat.setOutputPath(job, outPath);
	// // FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
	// // if (dfs.exists(outPath)) {
	// // dfs.delete(outPath, true);
	// // }
	// //
	// // try {
	// //
	// // job.waitForCompletion(true);
	// //
	// // } catch (InterruptedException ex) {
	// // Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
	// // null, ex);
	// // } catch (ClassNotFoundException ex) {
	// // Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
	// // null, ex);
	// // }
	//
	// }

	public static void main(String... args) throws Exception {
		runJob(args[0], args[1],args[2],args[3]);
	}

	public static void runJob(String input, String output,String start,String end) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", " ");
//		conf.set("xmlinput.start", "<text type=\"string\">");
//		conf.set("xmlinput.end", "</text>");
		conf.set("xmlinput.start", start);
		conf.set("xmlinput.end", end);

		Job job = new Job(conf);
		job.setJarByClass(ParserDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyParserMapper.class);
		//job.setReducerClass(MyParserReducer.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		


		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.waitForCompletion(true);
	}

}