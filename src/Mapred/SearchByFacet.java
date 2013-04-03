package Mapred;


import java.io.BufferedReader;
import java.io.FileReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SearchByFacet {

	public static void main(String... args) throws Exception {
		BufferedReader facetInput = new BufferedReader(new FileReader("Facets"));
		String line = facetInput.readLine();
		int num = 1;
        while (line != null) {
        		 // System.out.println(line + " Trimmed:"+ line.replaceAll(" ", ""));
        		  runJob(args[0], args[1], args[2], args[3], line+"\n" + line.replaceAll(" ", ""));
                  line = facetInput.readLine();
        }
		
		//runJob(args[0], args[1], args[2], args[3], args[4]);
	}

	public static void runJob(String input, String output, String start,
			String end, String el) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", " ");
		// conf.set("xmlinput.start", "<text type=\"string\">");
		// conf.set("xmlinput.end", "</text>");
		conf.set("xmlinput.start", start);
		conf.set("xmlinput.end", end);
		conf.set("xmlToSearch", el);
		conf.setLong("mapreduce.max.split.size", 67108864);
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);

		Job job = new Job(conf);
		job.setJarByClass(SearchByFacet.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompositeValueFormat.class);

		job.setMapperClass(MyParserMapper.class);
		job.setReducerClass(MyParserReducer.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(XmlInputFormatCombine.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
//		Path outPath = new Path(output);
		Path outPath = new Path(output+"-"+el.split("\n")[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.waitForCompletion(true);

	}
}
