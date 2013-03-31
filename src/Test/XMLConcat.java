package Test;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class XMLConcat {
    public static void main(String[] args) throws Throwable {
        File dir = new File("/home/ajay/Project/Tweets/");
        File[] rootFiles = dir.listFiles();

//        Writer outputWriter = new FileWriter("/home/ajay/Project/mergedFile.xml");
//        XMLOutputFactory xmlOutFactory = XMLOutputFactory.newFactory();
//        XMLEventWriter xmlEventWriter = xmlOutFactory.createXMLEventWriter(outputWriter);
//        XMLEventFactory xmlEventFactory = XMLEventFactory.newFactory();
//
//        xmlEventWriter.add(xmlEventFactory.createStartDocument());
//        xmlEventWriter.add(xmlEventFactory.createStartElement("", null, "rootSet"));
//
//        try{
//        XMLInputFactory xmlInFactory = XMLInputFactory.newFactory();
//        for (File rootFile : rootFiles) {
//        	System.out.println(rootFile.toString());
//            XMLEventReader xmlEventReader = xmlInFactory.createXMLEventReader(new StreamSource(rootFile));
//            XMLEvent event = xmlEventReader.nextEvent();
//            // Skip ahead in the input to the opening document element
//            while (event.getEventType() != XMLEvent.START_ELEMENT) {
//                event = xmlEventReader.nextEvent();
//            }
//
//            do {
//                xmlEventWriter.add(event);
//                event = xmlEventReader.nextEvent();
//            } while (event.getEventType() != XMLEvent.END_DOCUMENT);
//            xmlEventReader.close();
//        }
//        }catch(Exception e){
//        	e.printStackTrace();
//        }
//
//        xmlEventWriter.add(xmlEventFactory.createEndElement("", null, "rootSet"));
//        xmlEventWriter.add(xmlEventFactory.createEndDocument());
//
//        xmlEventWriter.close();
//        outputWriter.close();
//    	
    	
//    	File dir = new File("/tmp/rootFiles");
//    	String[] files = dir.list();
//    	if (files == null) {
//    	    System.out.println("No roots to merge!");
//    	} else {
//    	        try {
//    	        	FileChannel output = new FileOutputStream("output").getChannel()
//    	        	ByteBuffer buff = ByteBuffer.allocate(32);
//    	            
//    	            buff.put("<rootSet>\n".getBytes()); // specify encoding too
//    	            buff.flip();
//    	            output.write(buff);
//    	            buff.clear();
//    	            for (String file : files) 
//    	            {
//    	                try (FileChannel in = new FileInputStream(new File(dir, file).getChannel())) 
//    	                {
//    	                    in.transferTo(0, 1 << 24, output);
//    	                } catch (IOException e) {
//    	                    e.printStackTrace();
//    	                }
//    	            }
//    	            buff.put("</rootSet>\n".getBytes()); // specify encoding too
//    	            buff.flip();
//    	            output.write(buff);
//    	        } catch (IOException e) {
//    	            e.printStackTrace();
//    	        }
    	
    	
    	String[] filenames = new String[]{"/home/ajay/Project/To_Zip/303391120010911745.xml"};
    	try{
    		int x=0;
    		System.out.println(rootFiles.length + ":Length");
    		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream("/media/E48C7B648C7B3062/merged.xml"));
    	for (File filename : rootFiles) {
    		//System.out.println(filename.toString());
    	    InputStream inputStream = new BufferedInputStream(new FileInputStream(filename));
    	   // System.out.println(inputStream.available()+":Available");
    	    if(org.apache.commons.io.IOUtils.copy(inputStream, outputStream)==-1)
    	    	System.out.println("Fail");
    	    inputStream.close();
    	    x++;
    	}
    	outputStream.close();
    	System.out.println("Written:"+x);
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
}