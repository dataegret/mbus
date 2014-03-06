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

/**
 *
 * @author if
 */
public class ArrangedMessageProducer4MBUS extends MessageProducer4MBUS{
    final PreparedStatement firstPostSth;
    final PreparedStatement secondPostSth;
    
    protected ArrangedMessageProducer4MBUS(Connection conn, String qName, int messagesToSend) throws SQLException {
        super(conn, qName, messagesToSend,null);
        firstPostSth = conn.prepareStatement(String.format("select mbus.post('%s',hstore('key',?)) as iid", qName));
        secondPostSth = conn.prepareStatement(String.format(
                    "select mbus.post(qname:='%s',data:=hstore('key',?), headers:=hstore('consume_after',(array[?]::text)))",
                    qName)
        );
    }
    public static ArrangedMessageProducer4MBUS CreateArrangedMessageProducer4MBUS(Connection conn, String qName, int messagesToSend) throws SQLException{
        return new ArrangedMessageProducer4MBUS(conn, qName, messagesToSend);        
    }
    
    @Override
    public Integer call(){
        Exception caughtException=null;
        int messagesSent=0;
        boolean success = false;
        try{
            if(conn.getAutoCommit())
                conn.setAutoCommit(false);
            while(messagesToSend>0){
                if(Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Interrupted");
                firstPostSth.setString(1,java.lang.Integer.toString( messagesSent ));
                firstPostSth.execute();
                ResultSet iidRs = firstPostSth.getResultSet();
                if(!iidRs.next())
                    throw new RuntimeException("Cannot exec a post: have not got an iid");
                secondPostSth.setString(1, java.lang.Integer.toString( messagesSent ));
                secondPostSth.setString(2, iidRs.getString("iid"));
                secondPostSth.execute();
                messagesSent+=2;
                messagesToSend--;
            }
            success=true;
            conn.commit();
            return messagesSent;
        }catch(InterruptedException ex){
            return messagesSent;
        }catch(SQLException ex){
            caughtException = ex;
            System.err.println("SQL Error:"+ex.getMessage());
            throw new RuntimeException("SQL Error:" + ex.getMessage(), ex);
        }finally{
            try{
                if(!success)
                    conn.rollback();
            }catch(SQLException ex){
                if(caughtException!=null)
                    ex.initCause(caughtException);
                throw new RuntimeException("SQL Error: cannot commit:" + ex.getMessage(), ex);                
            }
        }
    }
}
