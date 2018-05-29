import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;
import java.util.Scanner;
import com.google.gson.*;

public class Client
{
	/**
	 * Class Members:
	 * dfs - a DFS object
	 */
    DFS dfs;
    
    /**
     * Client - a constructor
     * @param p - an integer argument
     * @throws Exception - in the case of invalid port number
     */
    public Client(int p) throws Exception 
    {
        dfs = new DFS(p);
    }
    
    /**
     * UI - a function that let's the user interact with the DFS.
     * @throws Exception - in case of complications communicating with the DFS
     */
    public void UI() throws Exception 
    {
        Scanner reader = new Scanner(System.in);
		System.out.println("Enter a command: (join, ls, touch, append, delete, read, tail, head, move, exit)");
		String command = reader.nextLine();
		while(true)
		{
			if (command.equals("join"))
			{
				String ip = "127.0.0.1";
				System.out.println("Enter a port:");
				int port = reader.nextInt();
				
				dfs.join(ip, port);
			}
			else if (command.equals("ls"))
			{
				dfs.ls();
			}
			else if (command.equals("touch"))
			{
				System.out.println("Enter a filename:");
				String filename = reader.nextLine();
				
				dfs.touch(filename);
			}
			else if (command.equals("append"))
			{
				System.out.println("Enter a filename:");
				String filename = reader.nextLine();
				System.out.println("What file (from Desktop) do you want to add?");
				String pathName = "C:\\Users\\Kyle Pamintuan\\Desktop\\" + reader.nextLine();
			    
			    dfs.append(filename, pathName);
			}
			else if (command.equals("delete"))
			{
				System.out.println("Enter a filename:");
				String filename = reader.nextLine();
				
				dfs.delete(filename);
			}
			else if (command.equals("move"))
			{
				System.out.println("Enter a filename:");
				String filename = reader.nextLine();
				System.out.println("Enter a new filename:");
				String new_filename = reader.nextLine();
				
				dfs.mv(filename, new_filename);
			}
			else if (command.equals("read"))
			{
				System.out.println("Enter an filename:");
				String filename = reader.nextLine();
				System.out.println("Enter a page number:");
				int pageNumber = reader.nextInt();
				
				dfs.read(filename, pageNumber);
			}
			else if (command.equals("head"))
			{
				System.out.println("Enter an filename:");
				String filename = reader.nextLine();
				
				dfs.head(filename);
			}
			else if (command.equals("tail"))
			{
				System.out.println("Enter an filename:");
				String filename = reader.nextLine();
				
				dfs.tail(filename);
			}
			else if (command.equals("exit"))
			{
				break;
			}
			else
			{
				System.out.println("That command does not exist. Try again.");
			}
			System.out.println("Enter a command: (join, ls, touch, delete, read, tail, head, append, move, exit)");
			command = reader.nextLine();
		}
		dfs.clearFiles();
		reader.close();
		System.exit(0);
    }
    
	/**
	 * Main Function:
	 * 1. Prompt user for a port number.
	 * 2. Initialize a Client object to participate in the DFS
	 * 3. Start the UI.
	 */
    static public void main(String args[]) throws Exception
    {  
    	Scanner reader = new Scanner(System.in);
		System.out.println("Enter port: ");
		int port = reader.nextInt();

		Client client = new Client(port);
		client.UI();
		
		reader.close();
    } 
}