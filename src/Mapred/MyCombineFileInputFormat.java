package Mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class MyCombineFileInputFormat extends CombineFileInputFormat <LongWritable, Text>{


    public static class myCombineFileRecordReader extends RecordReader<LongWritable, Text> {
        private LineRecordReader linerecord;
 
        private int index;
 
 
        public myCombineFileRecordReader(CombineFileSplit split,
                TaskAttemptContext context, Integer index)
            throws IOException, InterruptedException {
 
            this.index = index;
            InputSplit is = (InputSplit) split;
        }
 
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            CombineFileSplit combineSplit = (CombineFileSplit) split;
 
            linerecord = new LineRecordReader();
 
            FileSplit fileSplit = new FileSplit(combineSplit.getPath(index), combineSplit.getOffset(index), combineSplit.getLength(index), combineSplit.getLocations());
 
            linerecord.initialize(fileSplit, context);
        }
 
        @Override
        public void close() throws IOException {
            if (linerecord != null) {
                linerecord.close();
                linerecord = null;
            }
        }
 
        @Override
        public Text getCurrentValue() {
            return linerecord.getCurrentValue();
        }
 
        @Override
        public LongWritable getCurrentKey() {
            return linerecord.getCurrentKey();
        }
 
        @Override
        public float getProgress() throws IOException {
            return linerecord.getProgress();
        }
 
        @Override
        public boolean nextKeyValue() throws IOException {
            return linerecord.nextKeyValue();
        }
 
    }
  @Override
  public RecordReader<LongWritable, Text> createRecordReader (InputSplit split, TaskAttemptContext context) throws IOException {

      CombineFileSplit combineSplit = (CombineFileSplit) split;

      RecordReader<LongWritable, Text> rr = new CombineFileRecordReader<LongWritable, Text>(combineSplit, context, myCombineFileRecordReader.class);
      return rr;
  }

}