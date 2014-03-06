/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package name.shaif.MBUSTest1;

/**
 *
 * @author if
 */
public class WrongCommandLineArgument extends Exception {

    ExceptionKind ek = ExceptionKind.FATAL;
    /**
     * Creates a new instance of <code>WrongCommandLineArgument</code> without
     * detail message.
     */
    public WrongCommandLineArgument() {
    }

    /**
     * Constructs an instance of <code>WrongCommandLineArgument</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public WrongCommandLineArgument(String msg) {
        super(msg);
    }
    public WrongCommandLineArgument(String msg, Throwable e) {
        super(msg,e);
    }
}
