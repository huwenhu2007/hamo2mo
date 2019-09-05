package cn.com.manager;

import cn.com.manager.listenter.AppStartedListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.event.EventListener;

/**
 * @Author huwenhu
 * @Date 2019/8/23 14:09
 **/
@SpringBootApplication
public class MainApplication {

    public static void main(String[] args){
        SpringApplication.run( MainApplication.class );
    }


}
