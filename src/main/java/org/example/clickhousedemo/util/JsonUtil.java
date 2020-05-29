package org.example.clickhousedemo.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.IOException;

@Slf4j
public class JsonUtil {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String stringOfObject(Object object) {
        String ret = "{}";
        try {
            ret = objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Exception: ", e);
        }
        return ret;
    }

    public static <T> T objectOfFile(File file, Class<T> valueType) {
        T ret = null;
        try {
            ret = objectMapper.readValue(file, valueType);
        } catch (JsonParseException e) {
            log.error("Exception: ", e);
        } catch (JsonMappingException e) {
            log.error("Exception: ", e);
        } catch (IOException e) {
            log.error("Exception: ", e);
        }
        return ret;
    }

    public static <T> T objectOfPath(String path, Class<T> valueType) {
        File file = new File(path);
        return objectOfFile(file, valueType);
    }

    public static <T> T objectOfResourceFile(String fileName, Class<T> valueType) {
        Resource resource = new ClassPathResource(fileName);
        try {
            File file = resource.getFile();
            return objectOfFile(file, valueType);
        } catch (IOException e) {
            log.error("Exception: ", e);
        }
        return null;
    }
}
