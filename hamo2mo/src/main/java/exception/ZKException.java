package exception;

/**
 * @Author huwenhu
 * @Date 2019/8/16 17:11
 **/

public class ZKException extends Exception{

    public ZKException() {
    }

    public ZKException(Throwable cause) {
        super( cause );
    }

    public ZKException(String message) {
        super( message );
    }

    public ZKException(String message, Throwable cause) {
        super( message, cause );
    }
}
