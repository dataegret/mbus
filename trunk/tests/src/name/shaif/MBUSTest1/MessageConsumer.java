/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author if
 */
public class MessageConsumer implements Callable{
    final BlockingQueue<Message> q;
    private final AtomicInteger messagesReceived= new AtomicInteger(0);
    private final BigDecimal totalAmount = new BigDecimal(BigInteger.ZERO);

    private MessageConsumer(BlockingQueue<Message> q) {
        this.q = q;
    }
    public static MessageConsumer CreateMessageConsumer(BlockingQueue<Message> q){
        return new MessageConsumer(q);
    }
    
    @Override
    public Integer call(){
        try{
            while(true){
                Message message;
                message = q.take();
                if(message==null)
                    continue;
                
                messagesReceived.incrementAndGet();
                synchronized(totalAmount){
                    totalAmount.add(message.getAmount());
                }
            }
        }catch(InterruptedException  e){
            return getMessagesReceived();
        }
    }

    public int getMessagesReceived() {
        return messagesReceived.get();
    }

    public BigDecimal getTotalAmount() {
        synchronized(totalAmount){
            return totalAmount;
        }
    }
}
