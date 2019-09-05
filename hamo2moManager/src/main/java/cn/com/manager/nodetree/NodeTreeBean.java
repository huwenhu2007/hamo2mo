package cn.com.manager.nodetree;

import cn.com.manager.exception.ZKException;
import cn.com.manager.model.Node;
import cn.com.manager.utils.JSONObjectUtils;
import cn.com.manager.zk.ZKClient;
import cn.com.manager.zk.constans.NodeNameConstans;
import cn.com.manager.zk.constans.NodeState;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @Author huwenhu
 * @Date 2019/8/26 15:36
 **/
@Component
@Slf4j
public class NodeTreeBean {

    @Autowired
    private ZKClient zkClient;

    @Value( "#{${treeCache.treeCacheTime}}" )
    private int treeCacheTime;

    private static final String ROOT_NODE_NAME = "root";

    CacheLoader<String, Node> createCacheLoader() {
        return new CacheLoader<String, Node>() {
            @Override
            public Node load(String key) throws Exception {
                Node node = createCacheTree();
                return node;
            }
        };
    }

    LoadingCache<String, Node> cache = CacheBuilder.newBuilder()
            .expireAfterWrite( treeCacheTime, TimeUnit.SECONDS )
            .build(createCacheLoader());

    private Map<String, Node> nodeMap = null;

    public Node getTree(){
        Node node = null;
        try {
            node = cache.get( ROOT_NODE_NAME );
        } catch (ExecutionException e) {
            log.error( "获取缓存树异常: {}", e.getMessage(), e );
        }
        return node;
    }

    public Node getNodeByPath(String nodePath){
        return nodeMap.get( nodePath );
    }

    private Node createCacheTree() throws ZKException{
        String rootNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).toString();
        if(!zkClient.isExist( rootNodePath )){
            log.error( "{}根节点不存在", rootNodePath );
            return null;
        }
        nodeMap = new HashMap<>(  );

        Node rootNode = new Node(  );
        rootNode.setName( NodeNameConstans.ROOT.getName() );
        rootNode.setPath( rootNodePath );
        rootNode.setParentPath( "" );
        rootNode.setMessage( "" );
        rootNode.setOpen( false );

        List<Node> childNodeList = createNode( rootNode );
        rootNode.setChildren( childNodeList );

        nodeMap.put( rootNodePath, rootNode );

        setNodeAttribute();
//        log.info( "树信息：{}", rootNode.toJSONString() );
//        log.info( "树节点信息：{}", nodeMap.toString() );
        return rootNode;
    }

    private List<Node> createNode(Node node) throws ZKException {
        String rootNodePath = node.getPath();
        if(!zkClient.isExist( rootNodePath )){
            return null;
        }
        List<JSONObject> jsonObjectList =  zkClient.getChildNodeJSONDataList( rootNodePath );
        if(jsonObjectList.isEmpty()){
            return null;
        }

        List<Node> nodeList = new ArrayList<>(  );

        for(JSONObject jsonObject : jsonObjectList){
            String nodeName = jsonObject.getString( "nodeName" );
            Node childNode = new Node();
            childNode.setName( nodeName );
            childNode.setMessage( jsonObject.toString() );
            childNode.setOpen( false );

            String childNodePath = new StringBuilder( rootNodePath ).append( "/" ).append( nodeName ).toString();
            childNode.setPath( childNodePath );
            childNode.setParentPath( rootNodePath );

            List<Node> childrenNodeList = createNode( childNode );
            if(childrenNodeList != null){
                childNode.setChildren( childrenNodeList );
            }
            nodeList.add( childNode );
            nodeMap.put( childNodePath, childNode );
        }

        return nodeList;
    }


    private void setNodeAttribute(){

        for(String key : nodeMap.keySet()){

            if(key.endsWith( "state" )){
                Node node = getNodeByPath( key );
                JSONObject jsonObject = JSON.parseObject( node.getMessage() );
                JSONObject font = new JSONObject(  );
                if(getTaskState(jsonObject)){
                    font.put( "color", "green" );
                } else {
                    font.put( "color", "red" );
                }

                String parentNodePath = node.getParentPath();
                setParentColor(parentNodePath, font);
            }

        }
    }

    private void setParentColor(String nodePath, JSONObject font){
        Node node = getNodeByPath( nodePath );
        JSONObject f = new JSONObject(  );
        f.putAll( font );
        node.setFont( f );
        if(StringUtils.equals( "red", font.getString( "color" ) )){
            node.setOpen( true );
        }
        String parentNodePath = node.getParentPath();
        if(StringUtils.isNotEmpty( parentNodePath )){
            setParentColor(parentNodePath, font);
        }
    }

    private boolean getTaskState(JSONObject jsonObject){
        JSONObject stateJSONObject = jsonObject.getJSONObject( "state" );
        JSONObject tJSONObject = stateJSONObject.getJSONObject( "t" );
        boolean w = stateJSONObject.getBoolean( "w" );
        if(!w){
            return w;
        }
        boolean f = stateJSONObject.getBoolean( "f" );
        if(!f){
            return f;
        }
        for(String key : tJSONObject.keySet()){
            boolean t = tJSONObject.getBoolean( key );
            if(!t){
                return t;
            }
        }
        return true;
    }

    public void addNode(String nodePath, String message, String nodeName, String nodeType) throws ZKException{
        if(!JSONObjectUtils.isJSONData(message)){
            throw new ZKException("数据非json格式");
        }

        String parentNodePath = nodePath.substring( 0, nodePath.lastIndexOf( "/" ) );
        if (!zkClient.isExist( parentNodePath )) {
            throw new ZKException("父节点不存在");
        }

        JSONObject jsonObject = JSONObject.parseObject( message );
        if(StringUtils.equals( nodeType, "task" )){
            jsonObject.put( "nodeName", nodeName );
            jsonObject.put( "strWorkerId", nodeName );
            jsonObject.put( "runState", NodeState.READY.getState() );
        }
        if(StringUtils.equals( nodeType, "target" )){
            jsonObject.put( "nodeName", nodeName );
            jsonObject.put( "strSign", nodeName );
        }

        zkClient.createPersistentNode( nodePath,  jsonObject.toString());

        synchronized (ROOT_NODE_NAME) {
            cache.invalidate( ROOT_NODE_NAME );
        }
    }

    public void updateNode(String nodePath, String message) throws ZKException{
        if(!JSONObjectUtils.isJSONData(message)){
            throw new ZKException("数据非json格式");
        }
        if (!zkClient.isExist( nodePath )) {
            throw new ZKException("节点不存在");
        }
        Node node = getNodeByPath( nodePath );
        JSONObject jsonObject = JSON.parseObject( node.getMessage() );
        JSONObject update = JSON.parseObject( message );
        if(jsonObject.containsKey( "nodeName" )){
            update.put( "nodeName",  jsonObject.getString( "nodeName" ));
        }
        if(jsonObject.containsKey( "strWorkerId" )){
            update.put( "strWorkerId",  jsonObject.getString( "strWorkerId" ));
        }
        if(jsonObject.containsKey( "runState" )){
            update.put( "runState",  jsonObject.getString( "runState" ));
        }
        if(jsonObject.containsKey( "strSign" )){
            update.put( "strSign",  jsonObject.getString( "strSign" ));
        }
        if(jsonObject.containsKey( "strDMLTargetJSONArray" )){
            update.remove( "strDMLTargetJSONArray" );
        }
        zkClient.updateNode( nodePath, update.toString() );

        synchronized (ROOT_NODE_NAME) {
            cache.invalidate( ROOT_NODE_NAME );
        }
    }

    public void deleteNode(String nodePath) throws ZKException{
        zkClient.deleteNode( nodePath );

        synchronized (ROOT_NODE_NAME) {
            cache.invalidate( ROOT_NODE_NAME );
        }
    }

    public boolean onlyOperateState(String nodePath, String[] runStates){
        Node node = getNodeByPath( nodePath );
        JSONObject jsonObject = JSON.parseObject( node.getMessage() );
        String state = jsonObject.getString( "runState" );
        for(String runState : runStates){
            if(StringUtils.equals( runState, state )){
                return true;
            }
        }
        return false;
    }

    public void  changeTaskState(String nodePath, String runState) throws ZKException{
        Node node = getNodeByPath( nodePath );
        JSONObject jsonObject = JSON.parseObject( node.getMessage() );
        jsonObject.put( "runState", runState );
        zkClient.updateNode( nodePath, jsonObject.toString() );
        synchronized (ROOT_NODE_NAME) {
            cache.invalidate( ROOT_NODE_NAME );
        }
    }


    public boolean exist(String nodePath) throws ZKException{
        return zkClient.isExist( nodePath );
    }

}
