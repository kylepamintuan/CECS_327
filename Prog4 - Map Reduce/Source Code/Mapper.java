import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

public class Mapper implements MapReduceInterface 
{
    /**
     * map - is a function used to call emitMap.
     * @param key - a long argument
     * @param value - a string argument
     * @param context - a ChordMessageInterface argument
     * @throws IOException - in case of IO errors
     */
	public void map(Long key, String value, ChordMessageInterface context) throws IOException
	{
		context.emitMap(key, value);
	}
	
    /**
     * reduce - is a function used to call emitReduce.
     * @param key - a long argument
     * @param value - a List<String> argument
     * @param context - a ChordMessageInterface argument
     * @throws IOException - in case of IO errors
     */
	public void reduce(Long key, List<String> value, ChordMessageInterface context) throws IOException
	{
		//context.emitReduce(key, word +":"+ value.length());
	}
}
