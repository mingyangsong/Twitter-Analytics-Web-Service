
import java.io.*;
import java.net.*;
 
public class clientTest {
    public static void main(String[] args) throws IOException {
 
        Socket socket = null;
        PrintWriter out = null;
        BufferedReader in = null;
 
        try {
            socket = new Socket(args[0],Integer.parseInt(args[1]));
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection");
            System.exit(1);
        }
 
        BufferedReader read = new BufferedReader(new InputStreamReader(System.in));
        String num1,num2;
 
		//System.out.println(in.readLine()); //Uncomment to debug
 
		System.out.print("startRow");
		num1=args[2];
		out.println(num1);
		System.out.print("endRow");
		num2=args[3];
		out.println(num2);
		System.out.println("TotalCount:");
 
		System.out.println(in.readLine());
 
        out.close();
        in.close();
        read.close();
        socket.close();
    }
}
