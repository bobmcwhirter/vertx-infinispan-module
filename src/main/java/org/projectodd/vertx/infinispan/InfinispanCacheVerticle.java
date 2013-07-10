package org.projectodd.vertx.infinispan;

import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class InfinispanCacheVerticle extends Verticle {

    public static final String DEFAULT_ADDRESS = "org.projectodd.vertx.infinispan";
    public static final String PUT = DEFAULT_ADDRESS + ".put";
    public static final String GET = DEFAULT_ADDRESS + ".get";

    private DefaultCacheManager cacheManager;
    private Cache<Object, Object> cache;

    @Override
    public void start(Future<Void> startedResult) {
        this.cacheManager = new DefaultCacheManager();
        this.cacheManager.start();
        this.cache = cacheManager.getCache();
        this.cache.start();
        startedResult.setResult(null);
        vertx.eventBus().registerHandler(PUT, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> event) {
                String key = event.body().getString("key");
                Object value = event.body().getField("value");

                final NotifyingFuture<Object> putResult = InfinispanCacheVerticle.this.cache.putAsync(key, value);
                putResult.attachListener(new FutureListener<Object>() {
                    @Override
                    public void futureDone(java.util.concurrent.Future<Object> future) {
                        event.reply();
                    }
                });
            }
        });
        vertx.eventBus().registerHandler(GET, new Handler<Message<String>>() {
            @Override
            public void handle(final Message<String> event) {
                NotifyingFuture<Object> result = InfinispanCacheVerticle.this.cache.getAsync(event.body());
                result.attachListener(new FutureListener<Object>() {
                    @Override
                    public void futureDone(java.util.concurrent.Future<Object> future) {
                        try {
                            event.reply(future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
    }

    @Override
    public void stop() {
        this.cache.stop();
        this.cacheManager.stop();
        super.stop();
    }

}
