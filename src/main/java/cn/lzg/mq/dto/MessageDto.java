package cn.lzg.mq.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * @Author lzg
 * @Date 2016/12/24 13:30
 */
public class MessageDto{
    private int code;
    private String msg;
    private Date createTime;
    private boolean isUsed;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public boolean isUsed() {
        return isUsed;
    }

    public void setUsed(boolean used) {
        isUsed = used;
    }

    @Override
    public String toString() {
        return "code:"+code + ", msg:"+msg + ",createTime:"+createTime+",isUsed"+isUsed;
    }
}
