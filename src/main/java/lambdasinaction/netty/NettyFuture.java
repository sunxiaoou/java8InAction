package lambdasinaction.netty;

import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class NettyFuture {
    private static final Logger LOG = LoggerFactory.getLogger(NettyFuture.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        NioEventLoopGroup group = new NioEventLoopGroup();
        EventLoop eventLoop = group.next();
        Future<Integer> future = eventLoop.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                LOG.debug("doing");
                Thread.sleep(1000);
                return 43;
            }
        });
//        LOG.debug("waiting");
//        LOG.debug("done: {}", future.get());
        future.addListener(new GenericFutureListener<Future<? super Integer>>() {
            @Override
            public void operationComplete(Future<? super Integer> future) throws Exception {
                LOG.debug("done: {}", future.getNow());
            }
        });
    }
}
