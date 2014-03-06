/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.util.*;

/**
 *
 * @author if
 */
public class ThreadFactory implements java.util.concurrent.ThreadFactory {
    private List<Thread> threads = new ArrayList<Thread>();
    private List<Runnable> runnables = new ArrayList<>();

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        threads.add(thread);
        runnables.add(r);                
        return thread;
    }

    public List<Thread> getThreads() {
        return threads;
    }
    
}
