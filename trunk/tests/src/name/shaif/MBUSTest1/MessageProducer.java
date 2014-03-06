/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 *
 * @author if
 */
public class MessageProducer implements Callable<Integer> {
    final BlockingQueue<Message> q;
    final java.sql.Connection conn;
    private int messagesToSend;
    private int messagesSent=0;

    private MessageProducer(BlockingQueue<Message> q, Connection conn, int messagesToSend) {
        this.q = q;
        this.conn = conn;
        this.messagesToSend = messagesToSend;
    }
    
    public static MessageProducer CreateMessageProducer(BlockingQueue<Message> q, java.sql.Connection conn){
        return new MessageProducer(q, conn, 100);
    }
    
    public static MessageProducer CreateMessageProducer(BlockingQueue<Message> q, java.sql.Connection conn, int messagesToSend){
        return new MessageProducer(q, conn, messagesToSend);
    }
    
    @Override
    public Integer call(){
        try{
            while(messagesToSend>0){
                if(Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Got interrupt message");
                Message message = Message.CreateMessage("Left "+(messagesToSend-1) + " messages", new BigDecimal(100));
                q.put(message);
                messagesToSend--;
                messagesSent++;
            }
            conn.close();
        }catch(InterruptedException e){
            //do cleanup
            try{
                conn.close();
            }catch(SQLException ee){
                throw new RuntimeException("Cannot close connection", ee);
            }
            Thread.currentThread().interrupted();
            return messagesSent;
        }catch(Exception e){
            throw new RuntimeException(e.getMessage(),e);
        }
        return messagesSent;
    }
}
