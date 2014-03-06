/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.sql.*;

/**
 *
 * @author if
 */
public class ArrangedMessageConsumer4MBUS extends MessageConsumer4MBUS{
    final PreparedStatement insSth;
    final PreparedStatement seekSth;
    final PreparedStatement getMessageSth;
    private ArrangedMessageConsumer4MBUS(Connection conn, String qName) throws Exception{
        super(conn, qName, null);
        conn.setAutoCommit(true);
        insSth = conn.prepareStatement("insert into result_arrivedMessage(msgid) values(?)");
        seekSth = conn.prepareStatement("select msgid from result_arrivedMessage am where am.msgid=?");
        getMessageSth = conn.prepareStatement("select ((headers->'consume_after')::text[])[1] as consume_after, data->'key' as key, iid as iid from mbus.consume('"+qName+"');");
    }
    public static ArrangedMessageConsumer4MBUS CreateArrangedMessageConsumer4MBUS(Connection conn, String qName) throws Exception{
        return new ArrangedMessageConsumer4MBUS(conn, qName);
    }
    @Override
    public Integer call() throws Exception{
        Exception caughtException = null;
        boolean success = false;
        try{
            if(conn.getAutoCommit())
                conn.setAutoCommit(false);
            while(true){
                getMessageSth.execute();
                
                ResultSet rs = getMessageSth.getResultSet();
                if(!rs.next()){
                    rs.close();
                    Thread.sleep(20);
                    continue;
                }
                incrementMessagesReceived();
                if(rs.getString("consume_after")==null){
                    insSth.setString(1, rs.getString("iid"));
                    insSth.execute();
                }else{
                    seekSth.setString(1, rs.getString("consume_after"));
                    seekSth.execute();
                    ResultSet seekRs = seekSth.getResultSet();
                    try{
                        if(!seekRs.next() || !seekRs.getString("msgid").equals(rs.getString("consume_after"))){
                            System.out.println("Record not found:" + seekRs.getString("msgid") + " seek for" + rs.getString("consume_after"));
                            throw new Exception(String.format("Get unordered message with iid=%s",rs.getString("iid")));
                        }
                    }finally{
                        seekRs.close();
                    }
                }
                conn.commit();
                rs.close();
                success=true;
            }
        }catch(InterruptedException ex){
            success=true;
            return getMessagesReceived();
        }catch(SQLException ex){
            caughtException = ex;
            System.err.println("SQL error:" + ex.getMessage());
            throw new RuntimeException("SQL Error:" + ex.getMessage(), ex);
        }finally{
            try{
                if(!success)
                    conn.rollback();
                else
                    conn.commit();
            }catch(SQLException ex){
                if(caughtException!=null)
                    ex.initCause(caughtException);
                throw new RuntimeException("SQL Error:" + ex.getMessage(), ex);                
            }
        }
    }
}