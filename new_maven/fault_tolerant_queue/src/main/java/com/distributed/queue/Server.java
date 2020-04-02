package com.distributed.queue;

import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.distributed.queue.FTQueue;

public class Server extends Thread {

    private DatagramSocket socket;
    private boolean running;
    private byte[] buf = new byte[1024];
    private byte[] buf2 = new byte[1024];
    private int portNumber = 5000;
    private int message_id = 0;
    private List<Integer> portNumbers = null;
    private List<Integer> inactivePortNumbers = null;
    private FTQueue queue = null;
    private int sequenceNo = 0;
    private Map<String,String> bufferedMsg = null;  //<portno#msg_id, update_queue_msg>
    private Map<Integer,String> SequenceNoBuffer = null; // <SeqNo, portno#msg_id>
    private Map<String,String> deliveredMessages = null;
    private Map<Integer,String> deliveredSequenceNo = null;
    private List<String> messageIdForWhichSequenceNoIsSent = null; // Buffers messageIds for which sequence numbers is already sent, so we don't send it second time
    private boolean join = false; 
    public Server() {
        try {
            socket = new DatagramSocket(this.portNumber);
            List<Integer> portsList = Arrays.asList(5000,5001,5002,5003,5004);
            this.portNumbers = new ArrayList<>();
            this.inactivePortNumbers = new ArrayList<>();
            this.portNumbers.addAll(portsList);
            queue = new FTQueue();
            bufferedMsg = new HashMap<String,String>();
            SequenceNoBuffer = new HashMap<Integer,String>();
            deliveredMessages = new HashMap<String,String>();
            deliveredSequenceNo = new HashMap<Integer,String>();
            messageIdForWhichSequenceNoIsSent = new ArrayList<String>();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public Server(int portNumber) {
        this.portNumber = portNumber;
        try {
            socket = new DatagramSocket(this.portNumber);
            List<Integer> portsList = Arrays.asList(5000,5001,5002,5003,5004);
            this.portNumbers = new ArrayList<>();            
            this.inactivePortNumbers = new ArrayList<>();
            this.portNumbers.addAll(portsList);            
            queue = new FTQueue();
            bufferedMsg = new HashMap<String,String>();
            SequenceNoBuffer = new HashMap<Integer,String>();
            deliveredMessages = new HashMap<String,String>();
            deliveredSequenceNo = new HashMap<Integer,String>();
            messageIdForWhichSequenceNoIsSent = new ArrayList<String>();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
 
    public String updateQueue(String msg) {
        int label = 0;
        int queueId = 0;
        int element = 0;
        System.out.println("Message received in update queue:"+msg);
        String[] choices = msg.split(" ");
        if(choices.length < 2){
            System.out.println("Message not received completely:"+msg);
        }
        switch(Integer.parseInt(choices[1])) {
                case 1 :                    
                    label = Integer.parseInt(choices[2]);
                    return queue.qCreate(label);
                case 2 : 
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qDestroy(queueId);
                case 3 :
                    queueId = Integer.parseInt(choices[2]);
                    element = Integer.parseInt(choices[3]);
                    return queue.qPush(queueId, element);
                case 4 : 
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qPop(queueId);
                case 5 : 
                    label = Integer.parseInt(choices[2]);
                    return queue.qId(label);
                case 6 :
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qTop(queueId);
                case 7 :
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qSize(queueId); 
                default :
                    return "Please enter a valid choice";
        }
    }

    public void brodcastToServers(String broadcastMsg) {
        try {
            /* broadcast the message to all the server */
            DatagramPacket packet = null;
            InetAddress address = InetAddress.getByName("localhost");
            System.out.println("Broadcasting message:"+broadcastMsg); 
            byte[] buf3 = new byte[1024];           
            buf3 = broadcastMsg.getBytes();
            System.out.println("Broadcast msg converted to bytes"); 
            for(Integer port : this.portNumbers){
                //if(port != this.portNumber) {
                    System.out.println("Broadcasting to port number:"+port);
                    packet = new DatagramPacket(buf3, buf3.length, address, port);
                    socket.send(packet);
                //}            
            }
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public void brodcastClientMsg(String received) {
        try {
            this.message_id += 1;
            String messageId = this.portNumber+"#"+this.message_id;
            this.messageIdForWhichSequenceNoIsSent.add(messageId);
            String broadcastMsg = "broadcast-"+messageId;
            broadcastMsg = broadcastMsg + "-" +received;
            System.out.println("broadcastMsg:"+broadcastMsg);
            brodcastToServers(broadcastMsg);
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public void broadcastSequenceNumberToServers(String messageId) {
        brodcastToServers("SequenceNo-" + messageId +"-"+Integer.toString(this.sequenceNo));
    }

    public String bufferTheMessage(String msg) {
        String parts[] = msg.split("-");
        bufferedMsg.put(parts[1],parts[2]);
        for(Map.Entry<String,String> e : this.bufferedMsg.entrySet()){
            System.out.println("key:"+e.getKey()+"value:"+e.getValue());
        }
        if(!this.messageIdForWhichSequenceNoIsSent.contains(parts[1])) {
            checkSequenceNumberBroadcasting(parts[1]);
        }        
        return "Buffered the message";
    }

    public void replayMessages(InetAddress address, int port, String msg){
        try {
            String parts[] = msg.split("-");
            DatagramPacket packet = null;
            String broadcastMsg = "broadcast-"+this.deliveredSequenceNo.get(Integer.parseInt(parts[1])) +  "-" +this.deliveredMessages.get(this.deliveredSequenceNo.get(Integer.parseInt(parts[1])));
            buf = broadcastMsg.getBytes();
            packet = new DatagramPacket(buf, buf.length, address, port);
            socket.send(packet);
            String sequenceNoMsg = "SequenceNo-" + this.deliveredSequenceNo.get(Integer.parseInt(parts[1]) +"-"+parts[1]);
            buf = sequenceNoMsg.getBytes();
            packet = new DatagramPacket(buf, buf.length, address, port);
            socket.send(packet);            
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public void requestReplay( int sequenceNo) {
        try {
            /* broadcast the message to all the server */
            DatagramPacket packet = null;
            InetAddress address = InetAddress.getByName("localhost");     
            String Msg = "ReplayRequest-"+Integer.toString(sequenceNo);        
            buf = Msg.getBytes();
            int port = (sequenceNo%5000) + 5000;
            //if(port != this.portNumber) {
            packet = new DatagramPacket(buf, buf.length, address, port);
            socket.send(packet);
            //} 
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public String delivertheMessage(String msg) {
        if(msg == null) {
            return "Message is null or delivered the message";
        }
        String response = "";
        String parts[] = msg.split("-");
        int seqNo = Integer.parseInt(parts[2]);
        String messageId = parts[1];
        System.out.println("Sequence No:"+seqNo);
        if(seqNo == this.sequenceNo+1 || seqNo == this.sequenceNo) {            
            if(bufferedMsg.get(parts[1]) != null) {
                this.deliveredMessages.put(parts[1],bufferedMsg.get(parts[1]));
                this.deliveredSequenceNo.put(seqNo,parts[1]);
                response = updateQueue(bufferedMsg.get(parts[1]));
                this.sequenceNo +=1;
                bufferedMsg.remove(parts[1]);
                if(this.SequenceNoBuffer.containsKey(this.sequenceNo+1)) {
                    this.SequenceNoBuffer.remove(this.sequenceNo+1);
                    delivertheMessage(this.bufferedMsg.get(this.SequenceNoBuffer.get(this.sequenceNo+1)));                                
                }
            }
        }else{
            this.SequenceNoBuffer.put(seqNo,messageId);  //buffer the current <sequence_no,queue_msg_update>
            requestReplay(this.sequenceNo+1);            
            //request_replay <>
        }
        return response;
    }

    public void checkSequenceNumberBroadcasting(String messageId) {
        if((this.sequenceNo%5== (this.portNumber)%5000)||
        (this.inactivePortNumbers.contains(this.sequenceNo%5+5000) && ((this.sequenceNo+1)%5+5000 == this.portNumber))) {                        
            this.sequenceNo += 1;
            broadcastSequenceNumberToServers(messageId);                        
        }
    }

    public static boolean serverRunning(String host, int port){
        DatagramSocket s = null;
        try {
            s = new DatagramSocket(port);
            return true;
        }
        catch (Exception e) {
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

    public void checkHeartBeat(){
        List<Integer> li = new ArrayList<>();
        for(int i : this.portNumbers){
            li.add(i);
        }
        for(int port : li){
            if(serverRunning("localhost",port)){
                this.portNumbers.remove(new Integer(port));
                this.inactivePortNumbers.add(port);
            }
        }
    }

    public void replayLog(InetAddress add, int port){
        int len = 15;
        DatagramPacket packet = null;
        byte[] buf3 = null;
        try{
            for(int i=1; i<15; i++) {
                if(this.deliveredSequenceNo.containsKey(i)){
                    String messageId = this.deliveredSequenceNo.get(i);
                    buf3 = new byte[1024];

                    String msg = "broadcast-"+messageId+"-"+this.deliveredMessages.get(messageId);
                    //broadcast-5000#10-queue 3 1 11      
                    buf3 = msg.getBytes();
                    packet = new DatagramPacket(buf3, buf3.length, add, port);
                    socket.send(packet);
                    buf3 = new byte[1024];
                    msg = "SequenceNo-"+messageId+"-"+i;
                    //SequenceNo-5000#10-12
                    buf3 = msg.getBytes();
                    packet = new DatagramPacket(buf3, buf3.length, add, port);
                    socket.send(packet);
                    sleep(100);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void run() {
        running = true;        
        try {
            while (running) {
                if(this.join){
                    this.join = false;
                    DatagramPacket pac = null;
                    InetAddress add = InetAddress.getByName("localhost");
                    int p = 5000;
                    byte[] buf4 = new byte[1024];
                    String req = "LogReplay";
                    buf4 = req.getBytes();
                    pac = new DatagramPacket(buf4, buf4.length, add, p);
                    socket.send(pac);
                }
                byte[] buf3 = new byte[1024];
                String response = " ";
                DatagramPacket packet = new DatagramPacket(buf3, buf3.length);
                socket.receive(packet);
                InetAddress address = packet.getAddress();
                int port = packet.getPort();                
                String received = new String(packet.getData(), 0, packet.getLength());
                System.out.println("received: "+received);
                

                if (received.startsWith("queue")) {
                    String[] parts = received.split(" ");                    
                    if(Integer.parseInt(parts[1]) == 1 ) {
                        brodcastClientMsg(received);
                        checkSequenceNumberBroadcasting(this.portNumber+"#"+this.message_id);
                        response = updateQueue(received); 
                    }else if(Integer.parseInt(parts[1]) < 5) {
                        brodcastClientMsg(received);
                        checkSequenceNumberBroadcasting(this.portNumber+"#"+this.message_id);                        
                    } else {
                       response = updateQueue(received);                                       
                    }                   
                }else if(received.startsWith("broadcast")) {
                    checkHeartBeat();
                    bufferTheMessage(received);
                } else if(received.startsWith("SequenceNo")) {
                    response = delivertheMessage(received);
                } else if(received.startsWith("ReplayRequest")) {
                    replayMessages(address,port,received);
                } else if(received.startsWith("LogReplay")){
                    replayLog(address,port);
                }
                if(port != 5000 && port != 5001 && port != 5002 && port != 5003 && port != 5004) {
                    buf2 = response.getBytes();
                    packet = new DatagramPacket(buf2, buf2.length, address, port);
                    System.out.println("Sending packet to port number:"+port);
                    socket.send(packet);                
                }                
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        if(args.length < 1) {
            System.out.println("Sytntax for running the server: ./server <port_number>");
        }
        int port = Integer.parseInt(args[0]);

        
        Server udpServer = new Server(port);
        if(args.length == 2){
            String join = args[1];
            if(join.equals("join")){
                udpServer.join = true;
            }
        }
        udpServer.start();
    }
}