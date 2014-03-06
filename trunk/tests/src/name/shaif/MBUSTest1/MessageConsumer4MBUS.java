/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author if
 */
public class MessageConsumer4MBUS implements Callable{

    private final AtomicInteger messagesReceived= new AtomicInteger(0);
    protected Integer incrementMessagesReceived() {
        return messagesReceived.incrementAndGet();
    }

    Connection conn;
    PreparedStatement pollQueueSth;
    String qName;

    /**
     * 
     * @param conn JDBC Connection
     * @param qName queue name
     * @return new MessageConsumer4MBUS
     * @throws SQLException 
     */
    public static MessageConsumer4MBUS CreateMessageConsumer4MBUS(Connection conn, String qName) throws SQLException {
        return new MessageConsumer4MBUS(conn, qName, conn.prepareStatement("select * from mbus.consume(?)"));
    }
    protected MessageConsumer4MBUS(Connection conn, String qName, PreparedStatement sth) throws SQLException {
        this.conn = conn;
        conn.setAutoCommit(false);
        this.qName = qName;
        this.pollQueueSth = sth;
    }
    
    @Override
    public Integer call() throws Exception{
        int pollTriesCount=0;
        try{
            while(true){
                pollQueueSth.setString(1, qName);
                pollQueueSth.execute();
                pollTriesCount++;
                ResultSet rs = pollQueueSth.getResultSet();
                if(rs.next()){
                    incrementMessagesReceived();
                    rs.close();
                    if((pollTriesCount%10)==0)
                        conn.commit();
                    continue;
                }
                conn.commit();
                rs.close();
                Thread.sleep(20);
                if(Thread.currentThread().isInterrupted()){
                    throw new InterruptedException("Interrupted");
                }
            }            
        }catch(InterruptedException  e){
            try{ 
                pollQueueSth.close(); //conn.close();
            } catch(SQLException e2){ 
                throw new RuntimeException("Cannot close statement:"+e.getMessage(), e2);
            }
            return getMessagesReceived();
        }catch(SQLException e){            
            try{
                conn.rollback();
            }catch(SQLException e2){
                e2.initCause(e);
                throw new RuntimeException("Something goes wrong:"+e.getMessage(), e2);
            }
            System.out.println(e.getMessage());
            throw new RuntimeException("SQL Exceptions:" + e.getMessage(), e);
        }finally{
            try{ 
                pollQueueSth.close(); 
                conn.commit();
            }catch(SQLException e){ 
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public int getMessagesReceived() {
        return messagesReceived.get();
    }

}
