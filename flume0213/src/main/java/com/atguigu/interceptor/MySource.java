package com.atguigu.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    //前缀
    private String prefix;
    //间隔时间
    private Long interval;

    /**
     * 拉取事件并交给ChannelProcessor处理的方法
     *
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            //我们通过外部方法拉取数据
            Event e = getSomeData();

            // Store the Event into this Source's associated Channel(s)
            ChannelProcessor channelProcessor = getChannelProcessor();

            channelProcessor.processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        }
        return status;
    }

    /**
     * 拉取数据并包装成Event的过程
     *
     * @return 拉取到的Event
     */
    private Event getSomeData() throws InterruptedException {
        //通过随机数模拟拉取到的数据
        int i = (int) (Math.random() * 1000);

        //添加前缀
        String message = prefix + i;

        Thread.sleep(interval);

        //包装成Event
        Event event = new SimpleEvent();

        event.setBody(message.getBytes());

        return event;
    }

    /**
     * 如果拉取不到数据，Backoff时间的增长速度
     *
     * @return 增长量
     */
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    /**
     * 最大等待时间
     *
     * @return 时间
     */
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    /**
     * 来自于configurable, 可以定义我们的自定义Source
     *
     * @param context 配置文件
     */
    public void configure(Context context) {
        prefix = context.getString("prefff", "XXXX");
        interval = context.getLong("interval", 500L);
    }
}
