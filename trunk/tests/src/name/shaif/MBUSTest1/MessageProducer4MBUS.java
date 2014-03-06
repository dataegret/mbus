/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 *
 * @author if
 * Makes a series of messages to specified mbus queue
 */
public class MessageProducer4MBUS implements Callable<Integer>{
    final protected Connection conn;
    final private String qName;
    final PreparedStatement sth;
    int messagesToSend;
    
    /**
     * Create a MBUS message producer. It generates series of a random messages
     * and post it into specified queue.
     * @param conn connection to database
     * @param qName queue name in the database
     * @param messagesToSend number of generated messages to send
     * @return created producer
     */
    public static MessageProducer4MBUS CreateMessageProducer(Connection conn, String qName, int messagesToSend) throws SQLException {
        return new MessageProducer4MBUS(conn, qName, messagesToSend, conn.prepareStatement("select mbus.post(?,hstore(ARRAY[?,?,?,?]))"));
    }
    
    MessageProducer4MBUS(Connection conn, String qName, int messagesToSend, PreparedStatement sth) throws SQLException{
        this.conn = conn;
        if(conn.getAutoCommit())
            conn.setAutoCommit(false);
        this.qName = qName;
        this.messagesToSend = messagesToSend;
        this.sth = sth;
    }
    
    @Override
    public Integer call(){
        int messagesSent=0;
        Exception exc = null;
        try{
            while(messagesToSend>0){
                if(Thread.currentThread().isInterrupted())
                    throw new InterruptedException(qName);
                
                int pIndex=1;
                sth.setString(pIndex++, qName);
                
                sth.setString(pIndex++, "data");
                sth.setString(pIndex++, qName);
                
                sth.setString(pIndex++, "key");
                sth.setString(pIndex++, java.lang.Integer.toString(messagesToSend));
                
                sth.execute();
                sth.getResultSet().close();
                messagesSent++;
                messagesToSend--;
                if((messagesToSend%100)==0)
                    conn.commit();
            }
            conn.commit();
            conn.close();
        }catch(InterruptedException | SQLException ex){
            Thread.currentThread().interrupt();
            exc = ex;
            return 0;
        }finally{
            try { 
                if(Thread.currentThread().isInterrupted()){
                    conn.rollback(); 
                    //conn.close();
                }
            } catch(SQLException e2){ 
                e2.initCause(exc);
                throw new RuntimeException(e2);
            };            
            if(exc!=null)
                throw new RuntimeException("Cannot execute:" + exc.getMessage(), exc);
        }
        
        return messagesSent;        
    }
    
}
