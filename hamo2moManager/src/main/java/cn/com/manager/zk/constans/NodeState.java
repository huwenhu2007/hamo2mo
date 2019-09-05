package cn.com.manager.zk.constans;

/**
 * @Author huwenhu
 * @Date 2019/8/16 17:33
 **/
public enum NodeState {

    READY("ready"),
    START("start"),
    RUNNING("running"),
    EXCEPTION("exception"),
    STOP("stop"),
    SHUT("shut");

    private String state;

    NodeState(String state){
        this.state = state;
    }

    public String getState(){
        return state;
    }

}
