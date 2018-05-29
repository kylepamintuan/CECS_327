#include <iostream>			//cout
#include <string>
#include <stdio.h>			//printf
#include <stdlib.h>
#include <string.h>			//strlen
#include <sys/socket.h>     //socket
#include <arpa/inet.h>		//inet_addr
#include <netinet/in.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <fstream>			//file IO

#define BUFFER_LENGTH 2048
#define WAITING_TIME 150000

using namespace std;

/**
* 'create_connection' is a function used to connect to a server and return a socket pi.
* This function takes in two arguments (host and port) and returns an integer value (socket pi).
* @param host is a string value.
* @param port is an integer value.
* @return s is an integer value.
*/
int create_connection(string host, int port)
{
    int s;
    struct sockaddr_in socketAddress;

    memset(&socketAddress,0, sizeof(socketAddress));
    s = socket(AF_INET,SOCK_STREAM,0);
    socketAddress.sin_family=AF_INET;
    socketAddress.sin_port= htons(port);

    int a1,a2,a3,a4;

    if (sscanf(host.c_str(), "%d.%d.%d.%d", &a1, &a2, &a3, &a4) == 4)
    {
        // By IP
        socketAddress.sin_addr.s_addr =  inet_addr(host.c_str());
    }
    else
    {
        // By Name
        hostent *record = gethostbyname(host.c_str());
        in_addr *addressptr = (in_addr *)record->h_addr;
        socketAddress.sin_addr = *addressptr;
    }

    if(connect(s,(struct sockaddr *)&socketAddress,sizeof(struct sockaddr))==-1)
    {
        perror("connection fail");
        exit(1);
        return -1;
    }

    return s;
}

/**
* 'request' is a function used to send a request to an FTP server and return a response from that server.
* This function takes in two arguments (socket and message) and returns an integer value (FTP code).
* @param sock is an integer value.
* @param message is a string value.
* @return an integer value.
*/
int request(int sock, string message)
{
    return send(sock, message.c_str(), message.size(), 0);
}

/**
* 'reply' is a function used to receive data from a data server.
* This function takes in one argument (socket) and returns a string value (data).
* @param s is an integer value.
* @return strReply is a string value.
*/
string reply(int s)
{
    string strReply;
    int count;
    char buffer[BUFFER_LENGTH+1];
    usleep(WAITING_TIME);

    do
    {
        count = recv(s, buffer, BUFFER_LENGTH, 0);
        buffer[count] = '\0';
        strReply += buffer;
    }while (count ==  BUFFER_LENGTH);

    return strReply;
}

/**
* 'request_reply' is a function used to send a request to an FTP server and return a response from that server.
* This function takes in two arguments (socket and message) and returns a string value (server response).
* @param s is an integer value.
* @param message is a string value.
* @return a string value.
*/
string request_reply(int s, string message)
{
	if (request(s, message) > 0)
    	return reply(s);

	return "";
}

/**
* 'FTP_response' is a function used to read the server response and outputs a message to the user about the status of the request.
* This function takes in two arguments (message and FTP code).
* @param strReply is a string value.
* @param code is an integer value.
*/
void FTP_response(string strReply, int code)
{
    size_t found;

    if (code == 220)
    {
        found = strReply.find("220");
        if (found > 0)
            cout << "*** Server is NOT ready for new client. ***\n" << endl;
        else
            cout << "*** Server is ready for new client. ***\n" << endl;
    }
    else if (code == 150)
    {
        found = strReply.find("150");
        if (found > 0)
            cout << "*** Could not connect to data server. ***\n" << endl;
        else
            cout << "*** File status okay. About to open data connection. ***\n" << endl;
    }
    else if (code == 221)
    {
        found = strReply.find("221");
        if (found > 0)
            cout << "\n*** Service could NOT close control connection. ***" << endl;
        else
            cout << "\n*** Service closing control connection. ***" << endl;
    }
    else if (code == 331)
    {
        found = strReply.find("331");
        if (found > 0)
            cout << "\n*** Invalid username. ***\n" << endl;
        else
            cout << "\n*** Username ok, need password. ***\n" << endl;
    }
    else if (code == 230)
    {
        found = strReply.find("230");
        if (found > 0)
            cout << "\n*** Invalid password. ***\n" << endl;
        else
            cout << "\n*** Successfully logged in. ***\n" << endl;
    }
    else if (code == 226)
    {
        found = strReply.find("226");
        if (found > 0)
            cout << "\n*** File Transfer Incomplete. ***\n" << endl;
        else
            cout << "\n*** File Transfer Complete. ***\n" << endl;
    }
    else if (code == 250)
    {
        found = strReply.find("250");
        if (found > 0)
            cout << "\n*** Could NOT Change Directory. ***\n" << endl;
        else
            cout << "\n*** Change Directory Successful. ***\n" << endl;
    }
}

/**
* 'FTP_quit' is a function used to disconnect from the server
* This function takes in two arguments (message and FTP code).
* @param strReply is a string value.
* @param code is an integer value.
*/
void FTP_quit(int socket)
{
    string strReply;

    strReply = request_reply(socket, "QUIT\r\n");
    FTP_response(strReply, 221);
}

/*
    This function does the following:
        * Connects to the data server
        * Sends commands to the data server
        * Outputs the data being received from the server
*/
void FTP_function(string command, int socket, bool retr)
{
    int h1, h2, h3, h4, p1, p2, dataPort;
    string dataHost;
    string strReply;
    int sockpi_2;

    // Enter Passive Mode
    strReply = request_reply(socket, "PASV\r\n");
    sscanf((strReply).c_str(), "227 Entering Passive Mode (%d, %d, %d, %d, %d, %d)", &h1, &h2, &h3, &h4, &p1, &p2);

    // Connect to Data Server
    dataPort = ((p1 << 8) | p2);
    dataHost = to_string(h1) + "." + to_string(h2) + "." + to_string(h3) + "." + to_string(h4);
    sockpi_2 = create_connection(dataHost, dataPort);

    // Send Command to Data Server
    strReply = request_reply(socket, (command + "\r\n"));
    FTP_response(strReply, 150);

    // If "retr" command, then write the data server response to a file.
    // Otherwise, print the data server response.
    if (retr == true)
    {
        string filename = command.erase(0, 5);
        ofstream outputFile;
        outputFile.open(filename, ofstream::out);
        outputFile << reply(sockpi_2);
        outputFile.close();
    }
    else
        cout << reply(sockpi_2);

    // Close Connection w/ Data Server
    close(sockpi_2);
    FTP_response(reply(socket), 226);
}

int main(int argc , char *argv[])
{
    int sockpi;
    string strReply;

    // Create connection with server
    if (argc > 2)
        sockpi = create_connection(argv[1], atoi(argv[2]));
    if (argc == 2)
        sockpi = create_connection(argv[1], 21);
    else
        sockpi = create_connection("130.179.16.134", 21);
    cout << "Requesting Connection to FTP Server @ ftp.cc.umanitoba.ca (130.179.16.134)\n" << endl;
    strReply = reply(sockpi);
    FTP_response(strReply, 220);


    // Login
    string username;
    cout << "Username:" << endl;
    cin >> username; //username = "anonymous";
    strReply = request_reply(sockpi, "USER " + username + "\r\n");
    FTP_response(strReply, 331);
    string password;
    cout << "Password:" << endl;
    cin >> password; //password = "aaa@aaa.org";
    strReply = request_reply(sockpi, "PASS " + password + "\r\n");
    FTP_response(strReply, 230);

    // List files and directories in the server
    FTP_function("LIST", sockpi, false);

    // UI
    cout << "===== Command Menu =====" << endl;
    cout << "* ls = list files and directories" << endl;
    cout << "* cd = change directory" << endl;
    cout << "* retr = retrieve file" << endl;
    cout << "* quit = close connection with control server\n" << endl;
    string userInput;
    cout << "Ready for commands..." << endl;
    cin >> userInput;

    while (userInput != "quit")
    {
        // List files and directories
        if (userInput ==  "ls")
        {
            FTP_function("LIST", sockpi, false);
        }
        // Change directory
        else if (userInput == "cd")
        {
            string dir;
            cout << "Enter a directory" << endl;
            cin >> dir;

            strReply = request_reply(sockpi, "CWD " + dir + "\r\n");
            FTP_response(strReply, 250);
        }
        // Retrieve file
        else if (userInput == "retr")
        {
            string filename;
            cout << "Enter a filename" << endl;
            cin >> filename;

            FTP_function("RETR " + filename, sockpi, true);
        }

        // UI
        cout << "===== Command Menu =====" << endl;
        cout << "* ls = list files and directories" << endl;
        cout << "* cd = change directory" << endl;
        cout << "* retr = retrieve file" << endl;
        cout << "* quit = close connection with control server\n" << endl;
        cout << "Ready for commands..." << endl;
        cin >> userInput;
        cout << endl;
    }

    // Close connection to the sever
    FTP_quit(sockpi);

    return 0;
}
