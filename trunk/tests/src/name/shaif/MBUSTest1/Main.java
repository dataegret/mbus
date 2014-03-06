/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author if
 * Main class for test mbus for correctness and performance
 */
public class Main{
    static int TOTAL_PRODUCERS = 20;
    static int TOTAL_CONSUMERS = 7;
    static int TOTAL_TO_SEND = 1000;

    public static int getTOTAL_TO_SEND() {
        return TOTAL_TO_SEND;
    }
    static final String SELECTOR="((data->'key')::integer=100)";

    public static int getTOTAL_PRODUCERS() {
        return TOTAL_PRODUCERS;
    }

    public static int getTOTAL_CONSUMERS() {
        return TOTAL_CONSUMERS;
    }

    public static String getSELECTOR() {
        return SELECTOR;
    }

    public static String getNOT_SELECTOR() {
        return NOT_SELECTOR;
    }

    public static String getJDBC_URL() {
        return JDBC_URL;
    }

    public static String getJDBC_USER() {
        return JDBC_USER;
    }

    public static String getJDBC_PASSWORD() {
        return JDBC_PASSWORD;
    }
    static final String NOT_SELECTOR = "not(" + SELECTOR + ")";
    static String JDBC_URL = "jdbc:postgresql://localhost:5433/dst?prepareThreshold=1";
    static String JDBC_USER = "postgres";
    static String JDBC_PASSWORD = "root";
    
    private static void ParseCommandLine(String args[]) throws Exception{
        Map<String,String> cmdLine = new HashMap<String,String>();
        boolean first=true;
        String key = null;
        for(String s : args){
            if(first){
                if(s.matches("^[^-]"))
                    throw new WrongCommandLineArgument("Wrong switch:" + s);
                key = s;
                first = false;
                if(s.equals("-help"))
                    cmdLine.put(s,"-help");
            }else{
                assert key!=null;
                cmdLine.put(key, s);
                first = true;
            }
        }
        for(String s: cmdLine.keySet()){
            if(s.equals("-jdbc-url"))
                JDBC_URL = cmdLine.get(s);
            else if(s.equals("-jdbc-user"))
                JDBC_USER = cmdLine.get(s);
            else if(s.equals("-jdbc-password"))
                JDBC_PASSWORD = cmdLine.get(s);
            else if(s.equals("-producers")){
                TOTAL_PRODUCERS = Integer.parseInt(cmdLine.get(s));
                assert TOTAL_PRODUCERS > 0;
            } else if(s.equals("-consumers")){
                TOTAL_CONSUMERS = Integer.parseInt(cmdLine.get(s));
                assert TOTAL_CONSUMERS > 0;
            } else if(s.equals("-messages")){
                TOTAL_TO_SEND = Integer.parseInt(cmdLine.get(s));
                assert TOTAL_TO_SEND > 0;
            } else if(s.equals("-help")){
                System.out.println("Usage: java -jar MBUSTest1.jar [<options>]\n"
                        + "\t[-jdbc-url <jdbc-url>]\n"
                        + "\t[-jdbc-user <user>]\n"
                        + "\t[-jdbc-password <password>]\n"
                        + "\t[-producers <NUM>]\n"
                        + "\t[-consumers <NUM>\n"
                        + "\t[-help]");
                throw new ShowHelpException();
            }else
                throw new WrongCommandLineArgument("Unknown switch:"+s);
        }
    }
    
    public static void checkSentAndReceived(int sentCnt, int receivedCnt){
        System.out.format("Expected:%d consumed:%d", sentCnt, receivedCnt);
        if(sentCnt==receivedCnt)
            System.out.println("    +++ OK");
        else{
            System.out.format("    *** ERROR Sent:%d Received:%d", sentCnt, receivedCnt);
        }
    }

    public static void main(String[] args) throws Exception{        
        try{
            ParseCommandLine(args);
        }catch(ShowHelpException ex){
            return;
        }catch(Exception e){
            System.err.println(e.getMessage());
            return;
        }
        System.out.println("Plain queues test");
        long start1 = System.currentTimeMillis();
        mainMBUS();
        long end1  = System.currentTimeMillis();
        System.out.println("Plain queues OK:" + (end1-start1));
        System.out.println("Selector & consumer queues test");
        long start2 = System.currentTimeMillis();
        mainMBUSSelector();
        long end2 = System.currentTimeMillis();
        System.out.println("Selector & consumer queues test OK:"  + (end2-start2));
        System.out.println("Arranged test");
        long start3 = System.currentTimeMillis();
        mainMBUSArranged();
        long end3 = System.currentTimeMillis();
        System.out.println("Arranged test OK:"+(end3-start3));
    }
    
    static private String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder();
        for(byte b: a)
            sb.append(String.format("%02x", b&0xff));
        return sb.toString();
    }
   /**
    * Makes test for orderer messages.
    * @throws Exception 
    */ 
    public static void mainMBUSArranged() throws  Exception{
        String qName = "arrangedq" + byteArrayToHex(java.security.MessageDigest.getInstance("MD5").digest(ByteBuffer.allocate(8).putLong(Thread.currentThread().getId()).array()));
        qName = qName.substring(0, 31);

        Properties props = new Properties();
        props.setProperty("user",getJDBC_USER());
        props.setProperty("password",getJDBC_PASSWORD());
        String jdbcUrl = JDBC_URL;
        Connection conn = DriverManager.getConnection(getJDBC_URL(), props);
        conn.setAutoCommit(false);
        Statement preparationSth = conn.createStatement();
        preparationSth.execute("select mbus.create_queue('"+qName+"',256);");
        preparationSth.execute("drop table if exists result_arrivedMessage");
        preparationSth.execute("create table result_arrivedMessage(msgid text primary key)");
        conn.commit();
        
        try{            
            ThreadFactory producerThreads = new ThreadFactory();
            ExecutorService producerExec = Executors.newFixedThreadPool(getTOTAL_PRODUCERS(), producerThreads);
            List<Future<Integer>> producersResults = new ArrayList<Future<Integer>>();

            ThreadFactory consumerThreads = new ThreadFactory();
            ExecutorService consumerExec = Executors.newFixedThreadPool(getTOTAL_CONSUMERS(),consumerThreads);
            List<Future<Integer>> consumersResults = new ArrayList<Future<Integer>>();
            int i;

            for(i=0;i<getTOTAL_CONSUMERS();i++)
                consumersResults.add(consumerExec.submit(ArrangedMessageConsumer4MBUS.CreateArrangedMessageConsumer4MBUS(DriverManager.getConnection(jdbcUrl, props), qName)));
            
            for(i=0;i<getTOTAL_CONSUMERS();i++)
                producersResults.add(producerExec.submit(ArrangedMessageProducer4MBUS.CreateArrangedMessageProducer4MBUS(DriverManager.getConnection(jdbcUrl, props), qName, getTOTAL_TO_SEND())));

            int totalSend=0;
            for(Future<Integer> f: producersResults)
                totalSend+=f.get();

            conn.setAutoCommit(true);
            PreparedStatement sth=conn.prepareStatement("select 1 as has from mbus.qt$" + qName + " limit 1");
            while(true){
                sth.execute();
                ResultSet rs = sth.getResultSet();
                boolean rsnext = rs.next();
                rs.close();
                if(!rsnext)
                    break;
                Thread.sleep(20);
            }
            System.out.println("All received");


            for(Thread thr: consumerThreads.getThreads())
                thr.interrupt();

            int totalReceived=0;
            for(Future<Integer> f: consumersResults)
                totalReceived+=f.get();

            producerExec.shutdown();
            consumerExec.shutdown();

            checkSentAndReceived(totalSend, totalReceived);
        }finally{
            conn.setAutoCommit(false);
            preparationSth.executeQuery("select mbus.drop_queue('" + qName +"')");
            conn.commit();            
        }
    }
    
    /**
     * Makes test for several subscribers with two
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     * @throws NoSuchAlgorithmException 
     */
    public static void mainMBUSSelector() throws InterruptedException, ExecutionException, SQLException, NoSuchAlgorithmException{
        String qName = "testq" + byteArrayToHex(java.security.MessageDigest.getInstance("MD5").digest(ByteBuffer.allocate(8).putLong(Thread.currentThread().getId()).array()));
        qName = qName.substring(0, 31);
        System.out.println(qName);
                
        Properties props = new Properties();
        props.setProperty("user",getJDBC_USER());
        props.setProperty("password",getJDBC_PASSWORD());

        Connection initConn = DriverManager.getConnection(getJDBC_URL(), props);
        initConn.setAutoCommit(false);
        Statement preparationSth = initConn.createStatement();
        try{
            preparationSth.execute("select mbus.drop_queue('"+qName+"');");
        }catch(SQLException sqlEx){
            if(!sqlEx.getSQLState().equals("42P01"))
                throw sqlEx;
            initConn.rollback();
        }
        preparationSth.execute("select mbus.create_queue('"+qName+"',256);");
        initConn.commit();
        try{            
            preparationSth.execute(String.format("select mbus.create_consumer('cons1','%s',$STR$%s$STR$);",qName, getSELECTOR()));
            preparationSth.execute(String.format("select mbus.create_consumer('cons2','%s',$STR$%s$STR$);",qName, getNOT_SELECTOR()));
            initConn.commit();
        
            ThreadFactory producerThreads = new ThreadFactory();
            ExecutorService producerExec = Executors.newFixedThreadPool(getTOTAL_PRODUCERS(),producerThreads);
            List<Future<Integer>> producersResults = new ArrayList<Future<Integer>>();

            ThreadFactory consumerThreads = new ThreadFactory();
            ExecutorService consumerExec = Executors.newFixedThreadPool(getTOTAL_CONSUMERS()*3, consumerThreads);
            List<Future<Integer>> consumersResults = new ArrayList<Future<Integer>>();
            int i;

            for(i=0;i<getTOTAL_CONSUMERS();i++){
                consumersResults.add(consumerExec.submit(MessageConsumer4MBUSWithSelector.CreateMessageConsumer4MBUSWithSelector(DriverManager.getConnection(getJDBC_URL(), props), qName, "cons2" )));
                consumersResults.add(consumerExec.submit(MessageConsumer4MBUSWithSelector.CreateMessageConsumer4MBUSWithSelector(DriverManager.getConnection(getJDBC_URL(), props), qName, "cons1" )));
                consumersResults.add(consumerExec.submit(MessageConsumer4MBUSWithSelector.CreateMessageConsumer4MBUSWithSelector(DriverManager.getConnection(getJDBC_URL(), props), qName, "default" )));
            }

            for(i=0;i<getTOTAL_PRODUCERS();i++)
                producersResults.add(producerExec.submit(MessageProducer4MBUS.CreateMessageProducer(DriverManager.getConnection(getJDBC_URL(), props), qName, getTOTAL_TO_SEND())));

            int totalSend=0;
            for(Future<Integer> f: producersResults)
                totalSend+=f.get();

            producerExec.shutdown();
            consumerExec.shutdown();

            initConn.setAutoCommit(true);
            PreparedStatement sth=initConn.prepareStatement("select 1 as has from mbus.qt$" + qName + " limit 1");
            while(true){
                sth.execute();
                ResultSet rs = sth.getResultSet();
                boolean rsnext = rs.next();
                rs.close();
                if(!rsnext)
                    break;
                Thread.sleep(20);
            }


            for(Thread thr: consumerThreads.getThreads())
                thr.interrupt();

            int totalReceived=0;
            for(Future<Integer> f: consumersResults)
                totalReceived+=f.get();

            checkSentAndReceived(2*totalSend, totalReceived);
        }catch(SQLException e){
            System.err.println(e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }finally{
            initConn.setAutoCommit(false);
            preparationSth.executeQuery("select mbus.drop_queue('" + qName +"')");
            initConn.commit();
        }
    }
/**
 * 
 * @throws InterruptedException
 * @throws ExecutionException
 * @throws SQLException
 * @throws NoSuchAlgorithmException 
 */
    public static void mainMBUS() throws InterruptedException, ExecutionException, SQLException, NoSuchAlgorithmException{
        String qName = "mainq" + byteArrayToHex(java.security.MessageDigest.getInstance("MD5").digest(ByteBuffer.allocate(8).putLong(Thread.currentThread().getId()).array()));
        qName = qName.substring(0, 31);

        Properties props = new Properties();
        props.setProperty("user",getJDBC_USER());
        props.setProperty("password",getJDBC_PASSWORD());
        String jdbcUrl = getJDBC_URL();
        Connection initConn = DriverManager.getConnection(getJDBC_URL(), props);
        initConn.setAutoCommit(false);
        Statement preparationSth = initConn.createStatement();
        try{
            preparationSth.execute("select mbus.drop_queue('"+qName+"');");
        }catch(SQLException sqlEx){
            if(!sqlEx.getSQLState().equals("42P01"))
                throw sqlEx;
            initConn.rollback();
        }
        preparationSth.execute("select mbus.create_queue('"+qName+"',256);");
        initConn.commit();
        ExecutorService producerExec=null;
        ExecutorService consumerExec=null;
        try{
            for(int i=1; i<=getTOTAL_PRODUCERS();i++){
                Connection conn = DriverManager.getConnection(getJDBC_URL(), props);

                ThreadFactory producerThreads = new ThreadFactory();
                producerExec = Executors.newFixedThreadPool(getTOTAL_PRODUCERS(), producerThreads);
                List<Future<Integer>> producersResults = new ArrayList<Future<Integer>>();

                ThreadFactory consumerThreads = new ThreadFactory();
                consumerExec = Executors.newFixedThreadPool(getTOTAL_CONSUMERS(), consumerThreads);
                List<Future<Integer>> consumersResults = new ArrayList<Future<Integer>>();
                
                System.out.println("Going to work");

                for(i=0;i<getTOTAL_CONSUMERS();i++){
                    consumersResults.add(consumerExec.submit(MessageConsumer4MBUS.CreateMessageConsumer4MBUS(DriverManager.getConnection(getJDBC_URL(), props), qName)));
                }
                
                for(i=0;i<getTOTAL_PRODUCERS();i++)
                    producersResults.add(producerExec.submit(MessageProducer4MBUS.CreateMessageProducer(DriverManager.getConnection(getJDBC_URL(), props), qName, getTOTAL_TO_SEND())));

                producerExec.shutdown();
                consumerExec.shutdown();

                int totalSend=0;
                for(Future<Integer> f: producersResults)
                    totalSend+=f.get();

                conn.setAutoCommit(true);
                PreparedStatement sth=conn.prepareStatement("select 1 as has from mbus.qt$" + qName + " limit 1");
                while(true){
                    sth.execute();
                    ResultSet rs = sth.getResultSet();
                    boolean rsnext = rs.next();
                    rs.close();
                    if(!rsnext)
                        break;
                    Thread.sleep(10);
                }
                System.out.println("All received");

                for(Thread thr: consumerThreads.getThreads())
                    thr.interrupt();

                int totalReceived=0;
                for(Future<Integer> f: consumersResults)
                    totalReceived+=f.get();

                checkSentAndReceived(totalSend, totalReceived);
            }
        }finally{
            initConn.setAutoCommit(false);
            preparationSth.executeQuery("select mbus.drop_queue('" + qName +"')");
            initConn.commit();            
            if(producerExec!=null)
                producerExec.shutdown();
            
            if(consumerExec!=null)
                consumerExec.shutdown();
        }
    }
    public static void mainBlockingQueue() throws InterruptedException, ExecutionException{
        BlockingQueue<Message> q = new ArrayBlockingQueue<Message>(10, true);
        
        ThreadFactory producerThreads = new ThreadFactory();
        ExecutorService producerExec = Executors.newCachedThreadPool(producerThreads);
        List<Future<Integer>> producersResults = new ArrayList<Future<Integer>>();
        
        ThreadFactory consumerThreads = new ThreadFactory();
        ExecutorService consumerExec = Executors.newCachedThreadPool(consumerThreads);
        List<Future<Integer>> consumersResults = new ArrayList<Future<Integer>>();
        
        for(int i=0;i<getTOTAL_CONSUMERS();i++)
            consumersResults.add(consumerExec.submit(MessageConsumer.CreateMessageConsumer(q)));
        consumerExec.shutdown();
        
        for(int i=0;i<getTOTAL_PRODUCERS();i++)
            producersResults.add(producerExec.submit(MessageProducer.CreateMessageProducer(q,null,getTOTAL_TO_SEND())));
        producerExec.shutdown();

        int totalSend=0;
        
        for(Future<Integer> f: producersResults)
            totalSend+=f.get();                

        int totalReceived=0;        
                
        for(Thread thread : consumerThreads.getThreads())
            thread.interrupt();

        for(Future<Integer> f: consumersResults)
            totalReceived+=f.get();
        
        checkSentAndReceived(totalSend, totalReceived);
    }
}
