package cn.com.manager.service.impl;

import cn.com.manager.exception.ZKException;
import cn.com.manager.model.Node;
import cn.com.manager.nodetree.NodeTreeBean;
import cn.com.manager.service.IZKService;
import cn.com.manager.zk.ZKClient;
import cn.com.manager.zk.constans.NodeState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author huwenhu
 * @Date 2019/8/28 15:33
 **/
@Service(value = "zkService")
@Slf4j
public class ZKServiceImpl implements IZKService{

    @Autowired
    NodeTreeBean nodeTreeBean;

    public String getTree(){
        return nodeTreeBean.getTree().toJSONString();
    }


    public String addNode(String nodePath, String message, String nodeName, String nodeType) {
        try{
            if(nodeTreeBean.exist( nodePath )){
                return "节点已经存在";
            }

            nodeTreeBean.addNode( nodePath, message, nodeName, nodeType );
        } catch(Exception e){
            log.error( "节点新增异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }

    public String updateNode(String nodePath, String message) {
        try{
            String[] arrState = {NodeState.READY.getState(), NodeState.SHUT.getState(), NodeState.EXCEPTION.getState()};
            if(!nodeTreeBean.onlyOperateState( nodePath, arrState )){
                return "只有ready、shut、exception状态才能修改";
            }

            nodeTreeBean.updateNode( nodePath, message );
        } catch(Exception e){
            log.error( "节点修改异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }

    public String deleteNode(String nodePath) {
        try{
            String[] arrState = {NodeState.READY.getState(), NodeState.SHUT.getState(), NodeState.EXCEPTION.getState()};
            if(!nodeTreeBean.onlyOperateState( nodePath, arrState )){
                return "只有ready、shut、exception状态才能删除";
            }

            nodeTreeBean.deleteNode( nodePath );
        } catch(Exception e){
            log.error( "节点删除异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }


    public String start(String nodePath){
        try{
            String[] arrState = {NodeState.READY.getState(), NodeState.SHUT.getState(), NodeState.EXCEPTION.getState()};
            if(!nodeTreeBean.onlyOperateState( nodePath, arrState )){
                return "只有ready、shut、exception状态才能启动";
            }

            nodeTreeBean.changeTaskState( nodePath, "start" );
        } catch(Exception e){
            log.error( "节点任务启动异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }

    public String stop(String nodePath){
        try{
            String[] arrState = {NodeState.RUNNING.getState()};
            if(!nodeTreeBean.onlyOperateState( nodePath, arrState )){
                return "只有running状态才能停止";
            }

            nodeTreeBean.changeTaskState( nodePath, "stop" );
        } catch(Exception e){
            log.error( "节点任务停止异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }

    public String restart(String nodePath){
        try{
            String[] arrState = {NodeState.RUNNING.getState()};
            if(!nodeTreeBean.onlyOperateState( nodePath, arrState )){
                return "只有running状态才能重启";
            }

            nodeTreeBean.changeTaskState( nodePath, "restart" );
        } catch(Exception e){
            log.error( "节点任务重启异常:{}", e.getMessage(), e );
            return e.getMessage();
        }
        return "success";
    }


}
