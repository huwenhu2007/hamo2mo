package exception;

/**
 * @Author huwenhu
 * @Date 2019/8/21 14:32
 **/

public class MongoForwardException extends Exception{

    public MongoForwardException(String message) {
        super( message );
    }

    public MongoForwardException(String message, Throwable cause) {
        super( message, cause );
    }
}
