/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author if
 */
public class MessageConsumer4MBUSWithSelector extends MessageConsumer4MBUS{
    final String subscriberName;
    public static MessageConsumer4MBUSWithSelector CreateMessageConsumer4MBUSWithSelector(Connection conn, String qName, String subscriber) throws SQLException {
        return new MessageConsumer4MBUSWithSelector(conn, qName, subscriber);
    }
    private MessageConsumer4MBUSWithSelector(Connection conn, String qName, String subscriber) throws SQLException {
        super(conn,qName,conn.prepareStatement("select * from mbus.consume(?,'"+ subscriber + "');"));
        subscriberName=subscriber;
    }
    
}
