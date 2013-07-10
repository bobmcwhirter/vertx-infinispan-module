package org.projectodd.vertx.infinispan;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class InfinispanClient {

    private Vertx vertx;

    public InfinispanClient(Vertx vertx) {
        this.vertx = vertx;
    }

    public void put(final String key, final Object value, final Handler<Void> completionHandler) {
        vertx.eventBus().send(InfinispanCacheVerticle.PUT,
                new JsonObject()
                        .putString("key", key)
                        .putValue("value", value),
                new Handler<Message<Object>>() {
                    @Override
                    public void handle(Message<Object> event) {
                        completionHandler.handle(null);
                    }
                });
    }

    public <T> void get(final String key, final Handler<T> completionHandler) {
        vertx.eventBus().send(InfinispanCacheVerticle.GET, "item1", new Handler<Message<T>>() {
            @Override
            public void handle(Message<T> event) {
                completionHandler.handle(event.body());
            }
        });
    }
}
