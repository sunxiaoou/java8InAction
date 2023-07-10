package lambdasinaction.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class NettyPromise {
    private static final Logger LOG = LoggerFactory.getLogger(NettyPromise.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        DefaultPromise<Integer> promise = new DefaultPromise<>(group.next());

        new Thread(() -> {
            LOG.debug("doing");
            try {
                Thread.sleep(1000);
                promise.setSuccess(44);
            } catch (InterruptedException e) {
                e.printStackTrace();
                promise.setFailure(e);
            }
        }).start();

        LOG.debug("waiting");
        LOG.debug("done: {}", promise.get());
    }
}
