package cn.com.manager.exception;

/**
 * @Author huwenhu
 * @Date 2019/8/28 15:57
 **/

public class JSONDataException extends Exception{

    public JSONDataException(String message) {
        super( message );
    }

    public JSONDataException(String message, Throwable cause) {
        super( message, cause );
    }
}
