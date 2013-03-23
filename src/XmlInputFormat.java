//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DataOutputBuffer;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.InputSplit;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.RecordReader;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
//import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
//import org.apache.hadoop.mapred.lib.CombineFileSplit;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
////
////
////import org.apache.hadoop.conf.Configuration;
////import org.apache.hadoop.fs.*;
////import org.apache.hadoop.io.*;
////import org.apache.hadoop.mapreduce.*;
////import org.apache.hadoop.mapreduce.lib.input.*;
////import org.slf4j.*;
////
////import java.io.IOException;
//
////import java.io.IOException;
////
////import org.apache.hadoop.conf.Configuration;
////import org.apache.hadoop.fs.FSDataInputStream;
////import org.apache.hadoop.fs.FileSystem;
////import org.apache.hadoop.fs.Path;
////import org.apache.hadoop.io.DataOutputBuffer;
////import org.apache.hadoop.io.LongWritable;
////import org.apache.hadoop.io.Text;
////
////import org.apache.hadoop.mapred.JobConf;
////import org.apache.hadoop.mapred.Reporter;
////import org.apache.hadoop.mapreduce.InputSplit;
////import org.apache.hadoop.mapreduce.RecordReader;
////import org.apache.hadoop.mapreduce.TaskAttemptContext;
////import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
////import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
////import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
////import org.apache.hadoop.mapreduce.lib.input.FileSplit;
////import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
////import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
////import org.slf4j.Logger;
////import org.slf4j.LoggerFactory;
//
////import java.io.IOException;
////
////import org.apache.hadoop.conf.Configuration;
////import org.apache.hadoop.fs.FSDataInputStream;
////import org.apache.hadoop.fs.FileSystem;
////import org.apache.hadoop.fs.Path;
////import org.apache.hadoop.io.DataOutputBuffer;
////import org.apache.hadoop.io.LongWritable;
////import org.apache.hadoop.io.Text;
////import org.apache.hadoop.mapred.FileSplit;
////import org.apache.hadoop.mapred.InputSplit;
////import org.apache.hadoop.mapred.JobConf;
////import org.apache.hadoop.mapred.RecordReader;
////import org.apache.hadoop.mapred.Reporter;
////import org.apache.hadoop.mapred.TaskAttemptContext;
////import org.apache.hadoop.mapred.TextInputFormat;
////import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
////import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
////import org.apache.hadoop.mapred.lib.CombineFileSplit;
////import org.slf4j.Logger;
////import org.slf4j.LoggerFactory;
////
////
////
////
////
//////import org.apache.hadoop.conf.Configuration;
//////import org.apache.hadoop.fs.*;
//////import org.apache.hadoop.io.*;
//////import org.apache.hadoop.mapreduce.*;
//////import org.apache.hadoop.mapreduce.lib.input.*;
//////import org.slf4j.*;
//////
//////import java.io.IOException;
//////
///////**
////// * Reads records that are delimited by a specific begin/end tag.
////// */
//////public class XmlInputFormat extends TextInputFormat{
//////
//////  private static final Logger log =
//////      LoggerFactory.getLogger(XmlInputFormat.class);
//////
//////  public static final String START_TAG_KEY = "xmlinput.start";
//////  public static final String END_TAG_KEY = "xmlinput.end";
//////
//////  @Override
//////  public RecordReader<LongWritable, Text> createRecordReader(
//////      InputSplit split, TaskAttemptContext context) {
//////    try {
//////      return new XmlRecordReader((FileSplit) split,
//////          context.getConfiguration());
//////    } catch (IOException ioe) {
//////      log.warn("Error while creating XmlRecordReader", ioe);
//////      return null;
//////    }
//////  }
//////
//////  /**
//////   * XMLRecordReader class to read through a given xml document to
//////   * output xml blocks as records as specified
//////   * by the start tag and end tag
//////   */
//////  public static class XmlRecordReader
//////      extends RecordReader<LongWritable, Text> {
//////
//////    private final byte[] startTag;
//////    private final byte[] endTag;
//////    private final long start;
//////    private final long end;
//////    private final FSDataInputStream fsin;
//////    private final DataOutputBuffer buffer = new DataOutputBuffer();
//////    private LongWritable currentKey;
//////    private Text currentValue;
//////
//////    public XmlRecordReader(FileSplit split, Configuration conf)
//////        throws IOException {
//////      startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
//////      endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");
//////
//////      // open the file and seek to the start of the split
//////      start = split.getStart();
//////      end = start + split.getLength();
//////      Path file = split.getPath();
//////      FileSystem fs = file.getFileSystem(conf);
//////      fsin = fs.open(split.getPath());
//////      fsin.seek(start);
//////    }
//////
//////    private boolean next(LongWritable key, Text value)
//////        throws IOException {
//////      if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
//////        try {
//////          buffer.write(startTag);
//////          if (readUntilMatch(endTag, true)) {
//////            key.set(fsin.getPos());
//////            value.set(buffer.getData(), 0, buffer.getLength());
//////            return true;
//////          }
//////        } finally {
//////          buffer.reset();
//////        }
//////      }
//////      return false;
//////    }
//////
//////    @Override
//////    public void close() throws IOException {
//////      fsin.close();
//////    }
//////
//////    @Override
//////    public float getProgress() throws IOException {
//////      return (fsin.getPos() - start) / (float) (end - start);
//////    }
//////
//////    private boolean readUntilMatch(byte[] match, boolean withinBlock)
//////        throws IOException {
//////      int i = 0;
//////      while (true) {
//////        int b = fsin.read();
//////        // end of file:
//////        if (b == -1) {
//////          return false;
//////        }
//////        // save to buffer:
//////        if (withinBlock) {
//////          buffer.write(b);
//////        }
//////
//////        // check if we're matching:
//////        if (b == match[i]) {
//////          i++;
//////          if (i >= match.length) {
//////            return true;
//////          }
//////        } else {
//////          i = 0;
//////        }
//////        // see if we've passed the stop point:
//////        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
//////          return false;
//////        }
//////      }
//////    }
//////
//////    @Override
//////    public LongWritable getCurrentKey()
//////        throws IOException, InterruptedException {
//////      return currentKey;
//////    }
//////
//////    @Override
//////    public Text getCurrentValue()
//////        throws IOException, InterruptedException {
//////      return currentValue;
//////    }
//////
//////    @Override
//////    public void initialize(InputSplit split,
//////                           TaskAttemptContext context)
//////        throws IOException, InterruptedException {
//////    }
//////
//////    @Override
//////    public boolean nextKeyValue()
//////        throws IOException, InterruptedException {
//////      currentKey = new LongWritable();
//////      currentValue = new Text();
//////      return next(currentKey, currentValue);
//////    }
//////  }
//////}
////
////
///**
// * Reads records that are delimited by a specific begin/end tag.
// */
//public class XmlInputFormat extends TextInputFormat{
//
//  private static final Logger log =
//      LoggerFactory.getLogger(XmlInputFormat.class);
//
//  public static final String START_TAG_KEY = "xmlinput.start";
//  public static final String END_TAG_KEY = "xmlinput.end";
//
//  public RecordReader<LongWritable, Text> getRecordReader(
//      InputSplit split, JobConf job,Reporter reporter) {
//    try {
//      return new XmlRecordReader((FileSplit) split,
//          job.get(START_TAG_KEY).toString(),job.get(END_TAG_KEY).toString());
//    } catch (IOException ioe) {
//      log.warn("Error while creating XmlRecordReader", ioe);
//      return null;
//    }
//  }
//
//  /**
//   * XMLRecordReader class to read through a given xml document to
//   * output xml blocks as records as specified
//   * by the start tag and end tag
//   */
//  public static class XmlRecordReader implements RecordReader<LongWritable, Text> {
//
//    private final byte[] startTag;
//    private final byte[] endTag;
//    private final long start;
//    private final long end;
//    private final FSDataInputStream fsin;
//    private final DataOutputBuffer buffer = new DataOutputBuffer();
//    private LongWritable currentKey;
//    private Text currentValue;
//
//    public XmlRecordReader(FileSplit split, String starter,String ender)
//        throws IOException {
////      startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
////      endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");
//    	startTag = starter.getBytes();
//    	endTag = ender.getBytes();
//
//      // open the file and seek to the start of the split
//      start = split.getStart();
//      end = start + split.getLength();
//      Path file = split.getPath();
//      FileSystem fs = file.getFileSystem(conf);
//      fsin = fs.open(split.getPath());
//      fsin.seek(start);
//    }
//
//    @Override
//    public boolean next(LongWritable key, Text value)
//        throws IOException {
//      if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
//        try {
//          buffer.write(startTag);
//          if (readUntilMatch(endTag, true)) {
//            key.set(fsin.getPos());
//            value.set(buffer.getData(), 0, buffer.getLength());
//            return true;
//          }
//        } finally {
//          buffer.reset();
//        }
//      }
//      return false;
//    }
//
//    @Override
//    public void close() throws IOException {
//      fsin.close();
//    }
//
//    @Override
//    public float getProgress() throws IOException {
//      return (fsin.getPos() - start) / (float) (end - start);
//    }
//
//    private boolean readUntilMatch(byte[] match, boolean withinBlock)
//        throws IOException {
//      int i = 0;
//      while (true) {
//        int b = fsin.read();
//        // end of file:
//        if (b == -1) {
//          return false;
//        }
//        // save to buffer:
//        if (withinBlock) {
//          buffer.write(b);
//        }
//
//        // check if we're matching:
//        if (b == match[i]) {
//          i++;
//          if (i >= match.length) {
//            return true;
//          }
//        } else {
//          i = 0;
//        }
//        // see if we've passed the stop point:
//        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
//          return false;
//        }
//      }
//    }
//
//	@Override
//	public LongWritable createKey() {
//		// TODO Auto-generated method stub
//		return new LongWritable();
//	}
//
//	@Override
//	public Text createValue() {
//		// TODO Auto-generated method stub
//		return new Text();
//	}
//
//	@Override
//	public long getPos() throws IOException {
//		// TODO Auto-generated method stub
//		return fsin.getPos();
//	}
//  }
//}
//
//
//
//class MyCombineInputFormat1 extends CombineFileInputFormat<LongWritable,Text> {
//
//  public static class MyKeyValueLineRecordReader implements RecordReader {
//    private final RecordReader<LongWritable,Text> delegate;
//
//    public MyKeyValueLineRecordReader(
//      CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx) throws IOException {
//      FileSplit fileSplit = new FileSplit(
//        split.getPath(idx), split.getOffset(idx), split.getLength(idx), split.getLocations());
//      delegate = new org.apache.hadoop.mapred.LineRecordReader(conf, fileSplit);
//    }
//
//    
//    public boolean next(LongWritable key, Text value) throws IOException {
//      return delegate.next(key, value);
//    }
//
//    @Override
//    public LongWritable createKey() {
//      return delegate.createKey();
//    }
//
//    @Override
//    public Text createValue() {
//      return delegate.createValue();
//    }
//
//    @Override
//    public long getPos() throws IOException {
//      return delegate.getPos();
//    }
//
//    @Override
//    public void close() throws IOException {
//      delegate.close();
//    }
//
//    @Override
//    public float getProgress() throws IOException {
//      return delegate.getProgress();
//    }
//
//	@Override
//	public boolean next(Object arg0, Object arg1) throws IOException {
//		// TODO Auto-generated method stub
//		return false;
//	}
//  }
//
//  @Override
//  public RecordReader<LongWritable,Text> getRecordReader(
//    InputSplit split, JobConf job, Reporter reporter) throws IOException {
//    return new CombineFileRecordReader<LongWritable,Text>(
//      job, (CombineFileSplit) split, reporter, (Class) MyKeyValueLineRecordReader.class);
//  }
//
//}
//
////
/////**
//// * Reads records that are delimited by a specific begin/end tag.
//// */
////public class XmlInputFormat extends TextInputFormat{
////
////  private static final Logger log =
////      LoggerFactory.getLogger(XmlInputFormat.class);
////
////  public static final String START_TAG_KEY = "xmlinput.start";
////  public static final String END_TAG_KEY = "xmlinput.end";
////
////  @Override
////  public RecordReader<LongWritable, Text> createRecordReader(
////      InputSplit split, TaskAttemptContext context) {
////    try {
////      return new XmlRecordReader((FileSplit) split,
////          context.getConfiguration());
////    } catch (IOException ioe) {
////      log.warn("Error while creating XmlRecordReader", ioe);
////      return null;
////    }
////  }
////
////  /**
////   * XMLRecordReader class to read through a given xml document to
////   * output xml blocks as records as specified
////   * by the start tag and end tag
////   */
////  public static class XmlRecordReader
////      extends RecordReader<LongWritable, Text> {
////
////    private final byte[] startTag;
////    private final byte[] endTag;
////    private final long start;
////    private final long end;
////    private final FSDataInputStream fsin;
////    private final DataOutputBuffer buffer = new DataOutputBuffer();
////    private LongWritable currentKey;
////    private Text currentValue;
////
////    public XmlRecordReader(FileSplit split, Configuration conf)
////        throws IOException {
////      startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
////      endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");
////
////      // open the file and seek to the start of the split
////      start = split.getStart();
////      end = start + split.getLength();
////      Path file = split.getPath();
////      FileSystem fs = file.getFileSystem(conf);
////      fsin = fs.open(split.getPath());
////      fsin.seek(start);
////    }
////
////    private boolean next(LongWritable key, Text value)
////        throws IOException {
////      if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
////        try {
////          buffer.write(startTag);
////          if (readUntilMatch(endTag, true)) {
////            key.set(fsin.getPos());
////            value.set(buffer.getData(), 0, buffer.getLength());
////            return true;
////          }
////        } finally {
////          buffer.reset();
////        }
////      }
////      return false;
////    }
////
////    @Override
////    public void close() throws IOException {
////      fsin.close();
////    }
////
////    @Override
////    public float getProgress() throws IOException {
////      return (fsin.getPos() - start) / (float) (end - start);
////    }
////
////    private boolean readUntilMatch(byte[] match, boolean withinBlock)
////        throws IOException {
////      int i = 0;
////      while (true) {
////        int b = fsin.read();
////        // end of file:
////        if (b == -1) {
////          return false;
////        }
////        // save to buffer:
////        if (withinBlock) {
////          buffer.write(b);
////        }
////
////        // check if we're matching:
////        if (b == match[i]) {
////          i++;
////          if (i >= match.length) {
////            return true;
////          }
////        } else {
////          i = 0;
////        }
////        // see if we've passed the stop point:
////        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
////          return false;
////        }
////      }
////    }
////
////    @Override
////    public LongWritable getCurrentKey()
////        throws IOException, InterruptedException {
////      return currentKey;
////    }
////
////    @Override
////    public Text getCurrentValue()
////        throws IOException, InterruptedException {
////      return currentValue;
////    }
////
////    @Override
////    public void initialize(InputSplit split,
////                           TaskAttemptContext context)
////        throws IOException, InterruptedException {
////    }
////
////    @Override
////    public boolean nextKeyValue()
////        throws IOException, InterruptedException {
////      currentKey = new LongWritable();
////      currentValue = new Text();
////      return next(currentKey, currentValue);
////    }
////  }
////}
////
////

//package com.ajay;
//import java.io.IOException;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DataOutputBuffer;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//
///**
// * Reads records that are delimited by a specifc begin/end tag.
// */
//public class XmlInputFormat extends TextInputFormat {
//
//	public static final String START_TAG_KEY = "xmlinput.start";
//	public static final String END_TAG_KEY = "xmlinput.end";
//
//	@Override
//	public RecordReader<LongWritable, Text> createRecordReader(InputSplit is,
//			TaskAttemptContext tac) {
//
//		return new XmlRecordReader();
//
//	}
//
//	public static class XmlRecordReader extends
//			RecordReader<LongWritable, Text> {
//		private byte[] startTag;
//		private byte[] endTag;
//		private long start;
//		private long end;
//		private FSDataInputStream fsin;
//		private DataOutputBuffer buffer = new DataOutputBuffer();
//		private LongWritable key = new LongWritable();
//		private Text value = new Text();
//
//		@Override
//		public void initialize(InputSplit is, TaskAttemptContext tac)
//				throws IOException, InterruptedException {
//			FileSplit fileSplit = (FileSplit) is;
//			startTag = tac.getConfiguration().get(START_TAG_KEY)
//					.getBytes("utf-8");
//			endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");
//
//			start = fileSplit.getStart();
//			end = start + fileSplit.getLength();
//			Path file = fileSplit.getPath();
//
//			FileSystem fs = file.getFileSystem(tac.getConfiguration());
//			fsin = fs.open(fileSplit.getPath());
//			fsin.seek(start);
//
//		}
//
//		@Override
//		public boolean nextKeyValue() throws IOException, InterruptedException {
//			if (fsin.getPos() < end) {
//				if (readUntilMatch(startTag, false)) {
//					try {
//						buffer.write(startTag);
//						if (readUntilMatch(endTag, true)) {
//
//							value.set(buffer.getData(), 0, buffer.getLength());
//							key.set(fsin.getPos());
//							return true;
//						}
//					} finally {
//						buffer.reset();
//					}
//				}
//			}
//			return false;
//		}
//
//		@Override
//		public LongWritable getCurrentKey() throws IOException,
//				InterruptedException {
//			return key;
//		}
//
//		@Override
//		public Text getCurrentValue() throws IOException, InterruptedException {
//			return value;
//
//		}
//
//		@Override
//		public float getProgress() throws IOException, InterruptedException {
//			return (fsin.getPos() - start) / (float) (end - start);
//		}
//
//		@Override
//		public void close() throws IOException {
//			fsin.close();
//		}
//
//		private boolean readUntilMatch(byte[] match, boolean withinBlock)
//				throws IOException {
//			int i = 0;
//			while (true) {
//				int b = fsin.read();
//				// end of file:
//				if (b == -1)
//					return false;
//				// save to buffer:
//				if (withinBlock)
//					buffer.write(b);
//
//				// check if we're matching:
//				if (b == match[i]) {
//					i++;
//					if (i >= match.length)
//						return true;
//				} else
//					i = 0;
//				// see if we've passed the stop point:
//				if (!withinBlock && i == 0 && fsin.getPos() >= end)
//					return false;
//			}
//		}
//
//	}
//
//}

//
//import java.io.IOException;
//
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DataOutputBuffer;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.InputSplit;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.RecordReader;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextInputFormat;
//
///**
// * Reads records that are delimited by a specifc begin/end tag.
// */
//public class XmlInputFormat extends TextInputFormat {
//  
//  public static final String START_TAG_KEY = "xmlinput.start";
//  public static final String END_TAG_KEY = "xmlinput.end";
//  
//  @Override
//  public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
//                                                         JobConf jobConf,
//                                                         Reporter reporter) throws IOException {
//    return new XmlRecordReader((FileSplit) inputSplit, jobConf);
//  }
//  
//  /**
//   * XMLRecordReader class to read through a given xml document to output xml
//   * blocks as records as specified by the start tag and end tag
//   * 
//   */
//  public static class XmlRecordReader implements
//      RecordReader<LongWritable,Text> {
//    private final byte[] startTag;
//    private final byte[] endTag;
//    private final long start;
//    private final long end;
//    private final FSDataInputStream fsin;
//    private final DataOutputBuffer buffer = new DataOutputBuffer();
//    
//    public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
//      startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
//      endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
//      
//      // open the file and seek to the start of the split
//      start = split.getStart();
//      end = start + split.getLength();
//      Path file = split.getPath();
//      FileSystem fs = file.getFileSystem(jobConf);
//      fsin = fs.open(split.getPath());
//      fsin.seek(start);
//    }
//    
//    @Override
//    public boolean next(LongWritable key, Text value) throws IOException {
//      if (fsin.getPos() < end) {
//        if (readUntilMatch(startTag, false)) {
//          try {
//            buffer.write(startTag);
//            if (readUntilMatch(endTag, true)) {
//              key.set(fsin.getPos());
//              value.set(buffer.getData(), 0, buffer.getLength());
//              return true;
//            }
//          } finally {
//            buffer.reset();
//          }
//        }
//      }
//      return false;
//    }
//    
//    @Override
//    public LongWritable createKey() {
//      return new LongWritable();
//    }
//    
//    @Override
//    public Text createValue() {
//      return new Text();
//    }
//    
//    @Override
//    public long getPos() throws IOException {
//      return fsin.getPos();
//    }
//    
//    @Override
//    public void close() throws IOException {
//      fsin.close();
//    }
//    
//    @Override
//    public float getProgress() throws IOException {
//      return (fsin.getPos() - start) / (float) (end - start);
//    }
//    
//    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
//      int i = 0;
//      while (true) {
//        int b = fsin.read();
//        // end of file:
//        if (b == -1) return false;
//        // save to buffer:
//        if (withinBlock) buffer.write(b);
//        
//        // check if we're matching:
//        if (b == match[i]) {
//          i++;
//          if (i >= match.length) return true;
//        } else i = 0;
//        // see if we've passed the stop point:
//        if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
//      }
//    }
//  }
//}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.slf4j.*;

import java.io.IOException;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

  private static final Logger log =
      LoggerFactory.getLogger(XmlInputFormat.class);

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    try {
      return new XmlRecordReader((FileSplit) split,
          context.getConfiguration());
    } catch (IOException ioe) {
      log.warn("Error while creating XmlRecordReader", ioe);
      return null;
    }
  }

  /**
   * XMLRecordReader class to read through a given xml document to
   * output xml blocks as records as specified
   * by the start tag and end tag
   */
  public static class XmlRecordReader
      extends RecordReader<LongWritable, Text> {

    private final byte[] startTag;
    private final byte[] endTag;
    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;

    public XmlRecordReader(FileSplit split, Configuration conf)
        throws IOException {
      startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
      endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");

      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    private boolean next(LongWritable key, Text value)
        throws IOException {
      if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
        try {
          buffer.write(startTag);
          if (readUntilMatch(endTag, true)) {
            key.set(fsin.getPos());
            value.set(buffer.getData(), 0, buffer.getLength());
            return true;
          }
        } finally {
          buffer.reset();
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
        throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) {
          return false;
        }
        // save to buffer:
        if (withinBlock) {
          buffer.write(b);
        }

        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) {
            return true;
          }
        } else {
          i = 0;
        }
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
          return false;
        }
      }
    }

    @Override
    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split,
                           TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}


