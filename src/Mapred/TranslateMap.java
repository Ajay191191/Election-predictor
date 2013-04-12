package Mapred;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TranslateMap {

	private static int jobNo;

	public static void main(String... args) throws Exception {
		BufferedReader facetInput = new BufferedReader(new FileReader("Facets"));
		String line = facetInput.readLine();
		int num = 1;
		String search = new String();
		String parties = new String();
		boolean partyEnd = true;
		String people = new String();
		boolean peopleEnd = true;
		String hashTags = new String();
		boolean hashtagEnd = true;
		while (line != null) {
			if (line.equals("Parties")) {
				partyEnd = false;
				line = facetInput.readLine();
				continue;
			}
			if (line.equals("People")) {
				peopleEnd = false;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if (line.equals("Hashtag")) {
				hashtagEnd = false;
				peopleEnd = true;
				partyEnd = true;
				line = facetInput.readLine();
				continue;
			}
			if (!partyEnd) {
				parties += line + "\n";
			} else if (!peopleEnd) {
				people += line + "\n";
			} else if (!hashtagEnd) {
				hashTags += line + "\n";
			}
			// System.out.println(line + " Trimmed:"+ line.replaceAll(" ", ""));
			search += line + "\n";
			line = facetInput.readLine();
		}

		// Concat Files :

		File dir = new File("/home/ajay/Project/To_Zip");
		File[] rootFiles = dir.listFiles();

		File part0 = new File("/home/hduser/Tweets_Hadoop/part-r-00000");
		File part1 = new File("/home/hduser/Tweets_Hadoop/part-r-00001");

		File outputPart = null;
		InputStream inStream = null;
		OutputStream outStream = null;

		boolean mapReduceDone = false;
		try {
			int x = 0;
			System.out.println(rootFiles.length + ":Length");
			File outputFile = new File("Merged_Custom_Tag.xml");
			OutputStream outputStream = new BufferedOutputStream(
					new FileOutputStream(outputFile));
			for (File filename : rootFiles) {
				if (mapReduceDone) {
					outputFile.delete();
					outputFile.createNewFile();
					outputStream = new BufferedOutputStream(
							new FileOutputStream(outputFile));
					mapReduceDone = false;

				}
				
				File currentFile = new File(filename.toString() + "-done");
//				System.out.println(filename.getName());
				if(filename.getName().split("-").length > 1){
					continue;
				}
				InputStream inputStream = new BufferedInputStream(
						new FileInputStream(filename));
				// System.out.println(inputStream.available()+":Available");
				if (x == 0) {
					IOUtils.write("<Custom>", outputStream);
				}
				if (x % 10 == 0) {
					IOUtils.write("</Custom><Custom>", outputStream);
				}
				if (org.apache.commons.io.IOUtils.copy(inputStream,
						outputStream) == -1)
					System.out.println("Fail");
				inputStream.close();
				filename.renameTo(currentFile);
				x++;
				// if(x==750000){
				if (outputFile.length() / (1024 *1024 ) >= 100) {
					IOUtils.write("</Custom>", outputStream);
					outputStream.close();
					if (part0.exists()) {
						outputPart = new File("/home/ajay/Project/New/part-"
								+ jobNo + "-0.xml");
						inStream = new FileInputStream(part0);
						outStream = new FileOutputStream(outputPart);

						byte[] buffer = new byte[1024];

						int length;
						// copy the file content in bytes
						while ((length = inStream.read(buffer)) > 0) {

							outStream.write(buffer, 0, length);

						}

						inStream.close();
						outStream.close();
					}
					if (part1.exists()) {
						outputPart = new File("/home/ajay/Project/New/part-"
								+ jobNo + "-1.xml");
						inStream = new FileInputStream(part1);
						outStream = new FileOutputStream(outputPart);

						byte[] buffer = new byte[1024];

						int length;
						// copy the file content in bytes
						while ((length = inStream.read(buffer)) > 0) {

							outStream.write(buffer, 0, length);

						}

						inStream.close();
						outStream.close();
					}
					runJob("Tweets_custom", "outputCustom", "<Custom>",
							"</Custom>", search, parties, people, hashTags,
							"Merged_Custom_Tag.xml");
					jobNo++;
					mapReduceDone = true;
				}
			}
			IOUtils.write("</Custom>", outputStream);
			outputStream.close();
			if (part0.exists()) {
				outputPart = new File("/home/ajay/Project/New/part-"
						+ jobNo + "-0.xml");
				inStream = new FileInputStream(part0);
				outStream = new FileOutputStream(outputPart);

				byte[] buffer = new byte[1024];

				int length;
				// copy the file content in bytes
				while ((length = inStream.read(buffer)) > 0) {

					outStream.write(buffer, 0, length);

				}

				inStream.close();
				outStream.close();
			}
			if (part1.exists()) {
				outputPart = new File("/home/ajay/Project/New/part-"
						+ jobNo + "-1.xml");
				inStream = new FileInputStream(part1);
				outStream = new FileOutputStream(outputPart);

				byte[] buffer = new byte[1024];

				int length;
				// copy the file content in bytes
				while ((length = inStream.read(buffer)) > 0) {

					outStream.write(buffer, 0, length);

				}

				inStream.close();
				outStream.close();
			}
			runJob("Tweets_custom", "outputCustom", "<Custom>", "</Custom>",
					search, parties, people, hashTags, "Merged_Custom_Tag.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			if (part0.exists()) {
				outputPart = new File("/home/ajay/Project/New/part-"
						+ jobNo + "-0.xml");
				inStream = new FileInputStream(part0);
				outStream = new FileOutputStream(outputPart);

				byte[] buffer = new byte[1024];

				int length;
				// copy the file content in bytes
				while ((length = inStream.read(buffer)) > 0) {

					outStream.write(buffer, 0, length);

				}

				inStream.close();
				outStream.close();
			}
			if (part1.exists()) {
				outputPart = new File("/home/ajay/Project/New/part-"
						+ jobNo + "-1.xml");
				inStream = new FileInputStream(part1);
				outStream = new FileOutputStream(outputPart);

				byte[] buffer = new byte[1024];

				int length;
				// copy the file content in bytes
				while ((length = inStream.read(buffer)) > 0) {

					outStream.write(buffer, 0, length);

				}

				inStream.close();
				outStream.close();
			}
		}

		// runJob(args[0], args[1], args[2], args[3], search, parties,
		// people,hashTags);
		// runJob(args[0], args[1], args[2], args[3], args[4]);
	}

	public static void runJob(String input, String output, String start,
			String end, String el, String parties, String people,
			String hashTags, String localFile) throws Exception {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", " ");
		// conf.set("xmlinput.start", "<text type=\"string\">");
		// conf.set("xmlinput.end", "</text>");
		conf.set("xmlinput.start", start);
		conf.set("xmlinput.end", end);
		conf.set("xmlToSearch", el);
		conf.set("parties", parties);
		conf.set("people", people);
		conf.set("hashTags", hashTags);
		conf.setLong("mapreduce.max.split.size", 67108864);
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);

		FileSystem fs = FileSystem.get(conf);
		Path localFilePath = new Path(localFile);
		Path outputFilePath = new Path("Tweets_custom/");
		fs.copyFromLocalFile(true, localFilePath, outputFilePath);

		Job job = new Job(conf);
		job.setJarByClass(SearchByFacet.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompositeValueFormatTranslate.class);

		job.setMapperClass(TranslationMapper.class);
		// job.setCombinerClass(TranslateCombine.class);
		job.setReducerClass(TranslateReducer.class);
 
//		job.setNumReduceTasks(1);

		job.setInputFormatClass(XmlInputFormatCombine.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		// Path outPath = new Path(output+"-"+el.split("\n")[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		job.waitForCompletion(true);
		Path localOutputPath = new Path("/home/hduser/Tweets_Hadoop/");
		Path hadoopSrcPath = new Path("outputCustom/part-r-00000");
		Path hadoopSrcPath1 = new Path("outputCustom/part-r-00001");
		if (job.isComplete()) {
			if (fs.exists(hadoopSrcPath))
				fs.copyToLocalFile(true, hadoopSrcPath, localOutputPath);
			if (fs.exists(hadoopSrcPath1))
				fs.copyToLocalFile(true, hadoopSrcPath1, localOutputPath);
		}
	}
}
