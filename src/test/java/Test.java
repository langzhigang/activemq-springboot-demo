import cn.lzg.mq.Application;
import cn.lzg.mq.dto.MessageDto;
import cn.lzg.mq.producer.Producer;
import cn.lzg.mq.utils.ProtoStuffSerializerUtil;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

/**
 * @Author lzg
 * @Date 2016/12/23 23:53
 */
@RunWith(SpringRunner.class)

@SpringBootTest(classes = Application.class)
public class Test {

    @Autowired
    private Producer producer;

    @org.junit.Test
    public void testSendQueueMsg(){
        for(int i=0; i<10; i++){
            producer.sendQueueText("第："+ i +"跳sendQueueText测试消息");
        }

    }

    @org.junit.Test
    public void testSendTopicMsg(){
        for(int i=0; i<10; i++){
            producer.sendTopicText("第："+ i +"跳testSendTopicMsg测试消息");
        }
    }

    @org.junit.Test
    public void testSendQueueObjMsg(){
        MessageDto messageDto = null;
        for(int i=0; i<10; i++){
            messageDto = new MessageDto();
            messageDto.setCode(i);
            messageDto.setMsg("Obj消息"+i);
            messageDto.setCreateTime(new Date());
            messageDto.setUsed(false);
            producer.sendQueueObj(messageDto);
        }
    }
}
