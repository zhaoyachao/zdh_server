package com.zyc.common;


import com.mongodb.DBObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.spi.LoggingEvent;
import org.log4mongo.LoggingEventBsonifier;

public class ZdhMongoDbAppender extends org.log4mongo.MongoDbAppender {

    private LoggingEventBsonifier bsonifier = new MongoDbLoggingEventBsonifier();

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if (StringUtils.isNotBlank(loggingEvent.getLoggerName())
                && loggingEvent.getLoggerName().contains("com.zyc")) {
            DBObject bson = (DBObject)this.bsonifier.bsonify(loggingEvent);
            this.append(bson);
        }
    }
}