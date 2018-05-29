import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;

class MappingThread implements Runnable
{
	/**
	 * Class Members:
	 * t - a Thread object
	 * threadName - a String object
	 * guid - a long value
	 * mapper - a MapReduceInterface object
	 * context - a ChordMessageInterface object
	 */
	private Thread t;
	private String threadName;
	long guid;
	MapReduceInterface mapper;
	ChordMessageInterface context;
	
    /**
     * MappingThread - a constructor.
     * @param guid - a long argument
     * @param mapper - a MapReduceInterface argument
     * @param context - a ChordMessageInterface argument
     */
	MappingThread (Long guid, MapReduceInterface mapper, ChordMessageInterface context)
	{
		threadName = new Long(guid).toString();
		this.guid = guid;
		this.mapper = mapper;
		this.context = context;
		
		System.out.println("Creating Thread: " + threadName);
	}
	
    /**
     * run - is the function that is called once the thread is running.
     */
	public void run()
	{
		System.out.println("Running Thread: " +  threadName);
    	try 
    	{
        	InputStream file = context.get(guid);
        	InputStreamReader reader = new InputStreamReader(file);
    		
        	Long key;
        	String value;
        	
        	int t;
        	String line;
        	
        	for (int i = 0; i < 7; i++)
        	{
        		line = "";
        		
            	for (int j = 0; j < 120; j++)
            	{
            		t = reader.read();
            		char r = (char)t;
            		line += r;
            	}
            	
        		String[] parts = line.split(";");
        		String key_str = parts[0];

    			key = Long.parseLong(key_str);
    			value = parts[1];
    			
    			mapper.map(key, value, context);
    			
    			//System.out.println("[" + i + "] key = " + key + ", value = " + value);
        	}
        	reader.close();
		} 
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
	 }
	
    /**
     * start - is the function that is called once the thread starts.
     */
	public void start()
	{
	      System.out.println("Starting Thread: " +  threadName);
	      if (t == null) 
	      {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	}
}

public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface
{
    public static final int M = 2;
    
    Registry registry;    // rmi registry for lookup the remote objects.
    ChordMessageInterface successor;
    ChordMessageInterface predecessor;
    ChordMessageInterface[] finger;
    int nextFinger;
    long guid;   		// GUID (i)
    
    private Long numberOfRecords;
    private Set<Long> set;
    private Map<Long, List<String>> BMap;
    private Map<Long, String> BReduce;
    
	/**
	 * setWorkingPeer - is a function used to add pages to a working set.
	 * @param page - a long argument
	 * @throws IOExeption - in case of IO errors
	 */
    public void setWorkingPeer(Long page) throws IOException
    {
    	set.add(page);
    }
    
    /**
     * completePeer - is a function used to remove pages from a working set.
     * @param page - a long argument
     * @param n - a long argument
     * @throws RemoteException - in case of remote method errors
     */
    public void completePeer(Long page, Long n) throws RemoteException
    {
	    this.numberOfRecords += n;
	    set.remove(page);
    }
    
    /**
     * isPhaseComplete - is a function used notify if the working set is empty.
     * @throws IOExeption - in case of IO errors
     */
    public Boolean isPhaseCompleted() throws IOException
    {
    	return set.isEmpty();
    }
    
    /**
     * reduceContext - is a function used to perform the 'Reduce Phase' of Map Reduce.
     * @param source - a long argument
     * @param mapper - a MapReduceInterface argument
     * @param context - a ChordMessageInterface argument
     * @throws RemoteException - in case of remote method errors
     */
    public void reduceContext(Long source, MapReduceInterface reducer, ChordMessageInterface context) throws RemoteException
    {
    	// TODO
    }
    
    /**
     * mapContext - is a function used to perform the 'Map Phase' of Map Reduce.
     * @param guid - a long argument
     * @param mapper - a MapReduceInterface argument
     * @param context - a ChordMessageInterface argument
     * @throws RemoteException - in case of remote method errors
     */
    public void mapContext(Long guid, MapReduceInterface mapper, ChordMessageInterface context) throws RemoteException
    {
    	MappingThread m = new MappingThread(guid, mapper, context);
    	m.start();
    }
    
    /**
     * emitMap - is a function used to show results of 'Map Phase'.
     * @param key - a long argument
     * @param value - a string argument
     * @throws RemoteException - in case of remote method errors
     */
    public void emitMap(Long key, String value) throws RemoteException
    {
	    if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId()))
	    {
		    // insert in the BMap. Allows repetition
		    if (!BMap.containsKey(key))
		    {
			    List<String> list = new ArrayList< String >();
			    BMap.put(key,list);
		    }
		    BMap.get(key).add(value);
	    }
	    else
	    {
		    ChordMessageInterface peer = this.locateSuccessor(key);
		    peer.emitMap(key, value);
	    }
    }
    
    /**
     * emitReduce - is a function used to show results of 'Reduce Phase'.
     * @param key - a long argument
     * @param value - a string argument
     * @throws RemoteException - in case of remote method errors
     */
    public void emitReduce(Long key, String value) throws RemoteException
    {
	    if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId()))
	    {
		    // insert in the BReduce
		    BReduce.put(key, value);
	    }
	    else
	    {
		    ChordMessageInterface peer = this.locateSuccessor(key);
		    peer.emitReduce(key, value);
	    }
    }
    
    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2)
    {
       if (key1 < key2)
           return (key > key1 && key <= key2);
       else
          return (key > key1 || key <= key2);
    }

    public Boolean isKeyInOpenInterval(long key, long key1, long key2)
    {
    	if (key1 < key2)
          return (key > key1 && key < key2);
    	else
          return (key > key1 || key < key2);
    }
    
    
    public void put(long guidObject, InputStream stream) throws RemoteException 
    {
		try 
		{
		    String fileName = "./" + guid + "/repository/" + guidObject;
		    FileOutputStream output = new FileOutputStream(fileName);
		    while (stream.available() > 0)
		        output.write(stream.read());
		    output.close();
		}
		catch (IOException e) 
		{
		    System.out.println(e);
		}
    }
    
    
    public InputStream get(long guidObject) throws RemoteException 
    {
        FileStream file = null;
        try {
             file = new FileStream("./"+guid+"/repository/" + guidObject);
        } catch (IOException e)
        {
            throw(new RemoteException("File does not exists"));
        }
        return file;
    }
    
    public void delete(long guidObject) throws RemoteException 
    {
        File file = new File("./"+guid+"/repository/" + guidObject);
        file.delete();
    }
    
    public long getId() throws RemoteException 
    {
        return guid;
    }
    
    public boolean isAlive() throws RemoteException 
    {
	    return true;
    }
    
    public ChordMessageInterface getPredecessor() throws RemoteException 
    {
	    return predecessor;
    }
    
    public ChordMessageInterface locateSuccessor(long key) throws RemoteException 
    {
	    if (key == guid)
            throw new IllegalArgumentException("Key must be distinct that  " + guid);
	    if (successor.getId() != guid)
	    {
			if (isKeyInSemiCloseInterval(key, guid, successor.getId()))
			  return successor;
			ChordMessageInterface j = closestPrecedingNode(key);
			  
			if (j == null)
			  return null;
			return j.locateSuccessor(key);
        }
        return successor;
    }
    
    public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException 
    {
        if(key != guid) 
        {
            int i = M - 1;
            while (i >= 0) 
            {
                try
                {
       
                    if(isKeyInSemiCloseInterval(finger[i].getId(), guid, key)) 
                    {
                        if(finger[i].getId() != key)
                            return finger[i];
                        else 
                        {
                            return successor;
                        }
                    }
                }
                catch(Exception e)
                {
                    // Skip ;
                }
                i--;
            }
        }
        return successor;
    }
    
    public void joinRing(String ip, int port)  throws RemoteException 
    {
        try
        {
            System.out.println("Get Registry to joining ring");
            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));
            predecessor = null;
            successor = chord.locateSuccessor(this.getId());
            System.out.println("Joining ring");
        }
        catch(RemoteException | NotBoundException e){
            successor = this;
        }   
    }
    
    public void findingNextSuccessor()
    {
        int i;
        successor = this;
        for (i = 0;  i< M; i++)
        {
            try
            {
                if (finger[i].isAlive())
                {
                    successor = finger[i];
                }
            }
            catch(RemoteException | NullPointerException e)
            {
                finger[i] = null;
            }
        }
    }
    
    public void stabilize() 
    {
	  try 
	  {
	      if (successor != null)
	      {
	          ChordMessageInterface x = successor.getPredecessor();
	   
	          if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId()))
	          {
	              successor = x;
	          }
	          if (successor.getId() != getId())
	          {
	              successor.notify(this);
	          }
	      }
	  } 
	  catch(RemoteException | NullPointerException e1) 
	  {
	      findingNextSuccessor();
	
	  }
    }
    
    public void notify(ChordMessageInterface j) throws RemoteException 
    {
         if (predecessor == null || (predecessor != null
                    && isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
             predecessor = j;
            try 
            {
                File folder = new File("./"+guid+"/repository/");
                File[] files = folder.listFiles();
                for (File file : files) 
                {
                    long guidObject = Long.valueOf(file.getName());
                    if(guidObject < predecessor.getId() && predecessor.getId() < guid) 
                    {
                        predecessor.put(guidObject, new FileStream(file.getPath()));
                        file.delete();
                    }
                }
            } 
            catch (ArrayIndexOutOfBoundsException e) 
            {
                //happens sometimes when a new file is added during foreach loop
            } 
            catch (IOException e) 
            {
            e.printStackTrace();
            }

    }
    
    public void fixFingers() 
    {
    
        long id= guid;
        try 
        {
            long nextId = this.getId() + 1<< (nextFinger+1);
            finger[nextFinger] = locateSuccessor(nextId);
	    
            if (finger[nextFinger].getId() == guid)
                finger[nextFinger] = null;
            else
                nextFinger = (nextFinger + 1) % M;
        }
        catch(RemoteException | NullPointerException e)
        {
            e.printStackTrace();
        }
    }
    
    public void checkPredecessor() 
    { 	
      try 
      {
          if (predecessor != null && !predecessor.isAlive())
              predecessor = null;
      } 
      catch(RemoteException e) 
      {
          predecessor = null;
          // e.printStackTrace();
      }
    }
       
    public Chord(int port, long guid) throws RemoteException 
    {
        int j;
	    finger = new ChordMessageInterface[M];
        for (j=0;j<M; j++)
        {
	       finger[j] = null;
     	}
        this.guid = guid;
	
        predecessor = null;
        successor = this;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() 
        {
		    @Override
		    public void run() 
		    {
	            stabilize();
	            fixFingers();
	            checkPredecessor();
	        }
        }, 500, 500);
        
        try
        {
            // create the registry and bind the name and object.
            System.out.println("GUID: " + guid + " is starting RMI at port = " + port);
            registry = LocateRegistry.createRegistry( port );
            registry.rebind("Chord", this);
        }
        catch(RemoteException e)
        {
	       throw e;
        } 
    }
    
    void Print()
    {   
        int i;
        try 
        {
            if (successor != null)
                System.out.println("successor "+ successor.getId());
            if (predecessor != null)
                System.out.println("predecessor "+ predecessor.getId());
            for (i=0; i<M; i++)
            {
                try 
                {
                    if (finger != null)
                        System.out.println("Finger "+ i + " " + finger[i].getId());
                } 
                catch(NullPointerException e)
                {
                    finger[i] = null;
                }
            }
        }
        catch(RemoteException e)
        {
	       System.out.println("Cannot retrive id");
        }
    }
}
