package com.distributed.queue;


import java.net.*;
/**
 * Hello world!
 *
 */
public class App 
{
    public static boolean serverListening(String host, int port){
        DatagramSocket s = null;
        try {
            s = new DatagramSocket(port);
            return true;
        }
        catch (Exception e) {
            System.out.println(e);
            return false;
        }
        finally {
            if(s != null)
                try {
                    System.out.println("entered");
                    s.close();
                }
                catch(Exception e){}
        }
    }
    public static void main( String[] args )
    {
        System.out.println(serverListening("localhost", 5000));
    }
}
