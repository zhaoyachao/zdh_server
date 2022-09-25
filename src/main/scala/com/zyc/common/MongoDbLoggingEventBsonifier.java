package com.zyc.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.spi.LoggingEvent;
import org.log4mongo.LoggingEventBsonifierImpl;

import java.util.Date;

public class MongoDbLoggingEventBsonifier extends LoggingEventBsonifierImpl {

    @Override
    public DBObject bsonify(LoggingEvent loggingEvent) {
        BasicDBObject result = null;
        if (loggingEvent != null) {
            result = new BasicDBObject();
            String task_logs_id=String.valueOf(loggingEvent.getMDC("task_logs_id"));
            String job_id=String.valueOf(loggingEvent.getMDC("job_id"));

            result.put("task_logs_id", task_logs_id);
            result.put("job_id", job_id);
            result.put("log_time", new Date(loggingEvent.getTimeStamp()));
            this.nullSafePut(result, "level", loggingEvent.getLevel().toString());
            this.nullSafePut(result, "msg", loggingEvent.getMessage());
            this.addHostnameInformation(result);
        }
        return result;
    }

}