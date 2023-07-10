package lambdasinaction.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class JdkFuture {
    private static final Logger LOG = LoggerFactory.getLogger(JdkFuture.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Integer> future = service.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                LOG.debug("doing");
                Thread.sleep(1000);
                return 42;
            }
        });
        LOG.debug("waiting");
        LOG.debug("done: {}", future.get());
    }
}
