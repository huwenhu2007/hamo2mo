package cn.com.manager.model;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author huwenhu
 * @Date 2019/8/26 16:55
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Node {

    private String name;
    private String path;
    private String nodeType;
    private String parentPath;
    private String message;
    private boolean open;
    private JSONObject font;
    private List<Node> children;


    public String toJSONString(){
        return JSONObject.toJSONString( this );
    }

}
