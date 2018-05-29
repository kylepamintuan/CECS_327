import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.math.BigInteger;
import java.security.*;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;

public class DFS
{
	/**
	 * Class Members:
	 * port - an integer
	 * chord - a Chord object
	 */
    int port;
    Chord chord;
    
    /**
     * md5 - is a hash function.
     * @param objectName - a string argument
     * @return - the hash value
     */
    private long md5(String objectName)
    {
        try
        {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1,m.digest());
            return Math.abs(bigInt.longValue());
        }
        catch(NoSuchAlgorithmException e)
        {
                e.printStackTrace();
                
        }
        return 0;
    }
    
    /**
     * DFS - is a constructor.
     * @param port - an integer argument
     * @throws Exception - in case of invalid port number
     */
    public DFS(int port) throws Exception
    {
        this.port = port;
        long guid = md5("" + port);
        chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid+"/repository")); 
    }
    
    /**
     * join - is a function that uses the chord class to connect clients to a DFS.
     * @param Ip - an string argument
     * @param port - an integer argument
     * @throws Exception - in case of invalid port number or invalid ip address
     */
    public void join(String Ip, int port) throws Exception
    {
        chord.joinRing(Ip, port);
        chord.Print();
    }
    
    /**
     * readMetaData - is a function that retrieves metadata from Chord.
     * @return - a Metadata object
     * @throws Exception - in case no metadata is found
     */
    public Metadata readMetaData() throws Exception
    {
    	long guid = md5("Metadata");
    	ChordMessageInterface peer = chord.locateSuccessor(guid);
    	InputStream metadataraw = peer.get(guid);
    	
    	Gson gson = new Gson();
    	JsonReader reader = new JsonReader (new InputStreamReader(metadataraw, "UTF-8"));
    	Metadata metadata = gson.fromJson(reader, Metadata.class);
    	return metadata;
    }
    
    /**
     * writeMetaData - is a function that turns a metadata object into a json file before it is send to Chord.
     * @param metadata
     * @throws Exception
     */
    public void writeMetaData(Metadata metadata) throws Exception
    {
    	Gson gson = new Gson();
    	String metaString = gson.toJson(metadata);
    	InputStream stream = new ByteArrayInputStream(metaString.getBytes("UTF-8"));
    	String temp = "temp_metadata";
    	FileOutputStream output = new FileOutputStream(temp);
	    while (stream.available() > 0)
	        output.write(stream.read());
	    output.close();
    	writeMetaData(new FileStream(temp));
    }
    
    /**
     * writeMetaData - is an override function used to send metadata to Chord.
     * @param stream - an InputStream object
     * @throws Exception - in case no metadata is found
     */
    public void writeMetaData(InputStream stream) throws Exception
    {
    	long guid = md5("Metadata");
    	ChordMessageInterface peer = chord.locateSuccessor(guid);
    	peer.put(guid, stream);
    }
    
    /**
     * mv - is function used to rename a file in metadata.
     * @param filename - a string argument
     * @param newName - a string argument
     * @throws Exception - in case the file is not found
     */
    public void mv(String filename, String newName) throws Exception
    {
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
    		Metafile metafile = md.getFile(filename);
    		metafile.setName(newName);
    		writeMetaData(md);
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    }
    
    /**
     * touch - is a function used to create a file in metadata.
     * @param filename - a string argument
     * @throws Exception - in case the metadata is not found
     */
    public void touch(String filename) throws Exception
    {
    	Metadata md = readMetaData();
    	md.addFile(filename);
    	writeMetaData(md);
    }
    
    /**
     * ls - is a function that lists all files in metadata.
     * @throws Exception - in case the metadata is not found
     */
    public void ls() throws Exception
    {
		Metadata md = readMetaData();
    	if (md.files.size() > 0 || md == null)
    	{
            Metadata metadata = readMetaData();
            metadata.printListOfFiles();
    	}
    	else
    		System.out.println("No files found in metadata.");
    }
    
    /**
     * append - is a function that adds data to a file in metadata.
     * @param filename - a string argument
     * @param pathName - a string argument
     * @throws Exception - in case the metadata is not found
     */
    public void append(String filename, String pathName) throws Exception
    {
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
        	long guid = md5(pathName); // C:\Users\Kyle Pamintuan\Desktop\...
        	
        	FileStream real_file = new FileStream(pathName);
        	ChordMessageInterface peer = chord.locateSuccessor(guid);
        	peer.put(guid, real_file);
        	
        	Metafile metafile = md.getFile(filename);
        	metafile.addPage(guid);
        	writeMetaData(md);
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    }
    
    /**
     * delete - is a function that deletes a file in metadata.
     * @param filename - a string argument
     * @throws Exception - in case the metadata is not found
     */
    public void delete(String filename) throws Exception
    {
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
    		Metafile metafile = md.getFile(filename);
    		if(metafile.getNumOfPages() > 0)
    		{
	    		for (int i = 0; i < metafile.getNumOfPages(); i++)
	    		{
	    			Page page = metafile.pages.get(i);
	    			long guid = page.getGUID();
	    			ChordMessageInterface peer = chord.locateSuccessor(guid);
					peer.delete(guid);
	    		}
    		}
    		md.deleteFile(filename);
    		writeMetaData(md);
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    	
    }
    
    /**
     * read - is a function that reads data from a specified page in a specified file in metadata.
     * @param filename -  a string argument
     * @param pageNumber - an integer argument
     * @throws Exception - in case the metadata is not found
     */
    public void read(String filename, int pageNumber) throws Exception
    {   
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
    		Metafile metafile = md.getFile(filename);
    		Page page = metafile.getPage(pageNumber);
        	long guid = page.getGUID();
        	ChordMessageInterface peer = chord.locateSuccessor(guid);
        	InputStream metadataraw = peer.get(guid);
        	
        	int content;
			while ((content = metadataraw.read()) != 0) 
			{
				System.out.print((char) content);
			}
			System.out.println("");
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    }
    
    /**
     * tail - is a function that reads data from the last page in a specified file in metadata.
     * @param filename - a string argument
     * @throws Exception - in case the metadata is not found
     */
    public void tail(String filename) throws Exception
    {   
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
    		Metafile metafile = md.getFile(filename);
    		if (metafile.getNumOfPages() > 0)
    		{
        		Page page = metafile.getPage(metafile.getNumOfPages());
            	long guid = page.getGUID();
            	ChordMessageInterface peer = chord.locateSuccessor(guid);
            	InputStream metadataraw = peer.get(guid);
            	
            	int content;
    			while ((content = metadataraw.read()) != 0) 
    			{
    				System.out.print((char) content);
    			}
    			System.out.println("");
    		}
    		else
    			System.out.println("Could not find any pages under that filename");
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    }
    
    /**
     * head - is a function that reads data from the first page of a specified file in metadata.
     * @param filename - a string argument
     * @throws Exception - in case the metadata was not found
     */
    public void head(String filename) throws Exception
    {
    	Metadata md = readMetaData();
    	if (md.fileExists(filename))
    	{
    		Metafile metafile = md.getFile(filename);
    		if (metafile.getNumOfPages() > 0)
    		{
        		Page page = metafile.getPage(1);
            	long guid = page.getGUID();
            	ChordMessageInterface peer = chord.locateSuccessor(guid);
            	InputStream metadataraw = peer.get(guid);
            	
            	int content;
    			while ((content = metadataraw.read()) != 0) 
    			{
    				System.out.print((char) content);
    			}
    			System.out.println("");
    		}
    		else
    			System.out.println("Could not find any pages under that filename");
    	}
    	else
    		System.out.println("That file does not exist. Try again.");
    }
    
    /**
     * clearFiles - is a function that clears all files in the DFS.
     */
    public void clearFiles() throws Exception
    {
    	Metadata md = readMetaData();
    	md.clear();
    	writeMetaData(md);
    }  
}
