package cn.com.manager.controller;

import cn.com.manager.nodetree.NodeTreeBean;
import cn.com.manager.service.IZKService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * @Author huwenhu
 * @Date 2019/8/26 19:08
 **/
@Controller
@RequestMapping(value = "/zk")
public class TreeController {

    @Autowired
    IZKService zkService;

    @RequestMapping(value = "/getTree")
    @ResponseBody
    public String getTree(){
        return zkService.getTree();
    }

    @RequestMapping(value = "/demo")
    public String demo(){
        return "html/demo";
    }

    @RequestMapping(value = "/node")
    public String node(){
        return "html/node";
    }

    @RequestMapping(value = "/addNode")
    @ResponseBody
    public String addNode(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        String message = params.get( "message" );
        String nodeName = params.get( "nodeName" );
        String nodeType = params.get( "nodeType" );
        return zkService.addNode( nodePath, message, nodeName, nodeType );
    }

    @RequestMapping(value = "/updateNode")
    @ResponseBody
    public String updateNode(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        String message = params.get( "message" );
        return zkService.updateNode( nodePath, message );
    }

    @RequestMapping(value = "/deleteNode")
    @ResponseBody
    public String deleteNode(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        return zkService.deleteNode( nodePath );
    }

    @RequestMapping(value = "/start")
    @ResponseBody
    public String start(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        return zkService.start( nodePath );
    }

    @RequestMapping(value = "/stop")
    @ResponseBody
    public String stop(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        return zkService.stop( nodePath );
    }

    @RequestMapping(value = "/restart")
    @ResponseBody
    public String restart(@RequestBody Map<String, String> params){
        String nodePath = params.get( "path" );
        return zkService.restart( nodePath );
    }

}
