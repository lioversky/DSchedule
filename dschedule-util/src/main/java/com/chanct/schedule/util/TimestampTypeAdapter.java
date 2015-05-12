package com.chanct.schedule.util;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 *
 *
 * @author : lihx
 * create date : 2014-11-17
 */
public class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp>{   
    public JsonElement serialize(Timestamp src, Type arg1, JsonSerializationContext arg2) {   
    	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
        String dateFormatAsString = format.format(new Date(src.getTime()));   
        return new JsonPrimitive(dateFormatAsString);   
    }   
  
    public Timestamp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {   
        if (!(json instanceof JsonPrimitive)) {   
            throw new JsonParseException("The date should be a string value");   
        }   
  
        try {   
        	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
            Date date = (Date) format.parse(json.getAsString());   
            return new Timestamp(date.getTime());   
        } catch (Exception e) {   
            throw new JsonParseException(e);   
        }   
    }   

}  