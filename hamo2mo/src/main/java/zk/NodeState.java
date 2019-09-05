package zk;

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
    RESTART("restart"),
    SHUT("shut");

    private String state;

    NodeState(String state){
        this.state = state;
    }

    public String getState(){
        return state;
    }

}
