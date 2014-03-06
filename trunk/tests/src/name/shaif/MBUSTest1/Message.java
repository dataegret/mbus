/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 *
 * @author if
 */
public class Message {
    final private String description;
    final private BigDecimal amount;
    final private Calendar added;
    
    private Message(String description, BigDecimal amount, Calendar added){
        this.description = description;
        this.amount = amount;
        this.added = added;
    }
    
    static public Message CreateMessage(String description, BigDecimal amount, Calendar added){
        Message message = new Message(description, amount, added);
        return message;
    }
    static public Message CreateMessage(String description, BigDecimal amount){
        return CreateMessage(description, amount, new GregorianCalendar());
    }
    static public Message CreateMessage(BigDecimal amount){
        return CreateMessage("", amount);
    }

    public String getDescription() {
        return description;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Calendar getAdded() {
        return added;
    }
    
}
