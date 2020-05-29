package org.example.clickhousedemo.http;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class ResponseMessage {
    Integer code;
    String message;
    Object data;

    public static ResponseMessage success() {
        return ResponseMessage.success(null);
    }


    public static ResponseMessage success(Object data) {
        ResponseMessage ret = new ResponseMessage();
        ret.code = 0;
        ret.message = "Succeed";
        ret.data = data;
        return ret;
    }


    public static ResponseMessage fail() {
        return ResponseMessage.fail("Failed");
    }

    public static ResponseMessage fail(String message) {
        return ResponseMessage.fail(message, null);
    }

    public static ResponseMessage fail(Throwable t) {
        return ResponseMessage.fail(t.getLocalizedMessage());
    }

    public static ResponseMessage fail(String message, Object data) {
        ResponseMessage ret = new ResponseMessage();
        ret.code = -1;
        ret.message = message;
        ret.data = data;
        return ret;
    }


    @JsonIgnore
    public boolean isSucceed() {
        return code == 0;
    }
}
