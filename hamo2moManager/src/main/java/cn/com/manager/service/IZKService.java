package cn.com.manager.service;

import cn.com.manager.exception.ZKException;

/**
 * @Author huwenhu
 * @Date 2019/8/28 15:33
 **/

public interface IZKService {

    public String getTree();

    public String addNode(String nodePath, String message, String nodeName, String nodeType) ;

    public String updateNode(String nodePath, String message) ;

    public String deleteNode(String nodePath) ;

    public String start(String nodePath);

    public String stop(String nodePath);

    public String restart(String nodePath);


}
