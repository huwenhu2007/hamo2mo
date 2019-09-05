package cn.com.manager.exception;

/**
 * @Author huwenhu
 * @Date 2019/8/30 15:28
 **/

public class NodeSwitchException extends Exception{

    public NodeSwitchException(Throwable cause) {
        super( cause );
    }

    public NodeSwitchException(String message, Throwable cause) {
        super( message, cause );
    }
}
