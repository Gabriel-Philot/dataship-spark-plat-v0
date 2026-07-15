package com.dataship.spark.observability;

import org.apache.spark.SparkFirehoseListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPlatformFirehoseListener extends SparkFirehoseListener {
    private static final Logger LOG = LoggerFactory.getLogger(SparkPlatformFirehoseListener.class);

    public SparkPlatformFirehoseListener() {
        LOG.info("DATASHIP_LISTENER initialized constructor=noarg");
    }

    @Override
    public void onEvent(SparkListenerEvent event) {
        if (event == null) {
            return;
        }
        LOG.info("DATASHIP_LISTENER event_class={}", event.getClass().getName());
    }
}
