package zk;

/**
 * 进程节点
 * @Author huwenhu
 * @Date 2019/8/16 16:50
 **/
public enum NodeNameConstans {

    RUNNABLE_NODE("runnable"),
    READY_NODE("ready"),
    PROCESS_LIFE_MONITER_NODE("lifeMoniter");

    private String name;

    NodeNameConstans(String name){
        this.name = name;
    }

    public String getName(){
        return name;
    }


    public static void main(String[] args){
        System.out.println(NodeNameConstans.RUNNABLE_NODE.getName());
    }

}
