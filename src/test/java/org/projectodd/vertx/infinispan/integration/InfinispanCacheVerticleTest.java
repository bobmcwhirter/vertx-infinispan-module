package org.projectodd.vertx.infinispan.integration;

import org.junit.Test;
import org.projectodd.vertx.infinispan.InfinispanCacheVerticle;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

public class InfinispanCacheVerticleTest extends TestVerticle {
    
    static {
        System.setProperty("jgroups.bind_addr", "127.0.0.1" );
    }

    @Override
    public void start() {
        container.deployModule(System.getProperty("vertx.modulename"), 2, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                initialize();
                VertxAssert.assertTrue(asyncResult.succeeded());
                if (!asyncResult.succeeded()) {
                    asyncResult.cause().printStackTrace();
                }
                VertxAssert.assertNotNull("deploymentID should not be null", asyncResult.result());
                startTests();
            }
        });
    }

    @Test
    public void testRaw() throws InterruptedException {
        Thread.sleep( 5000 );
        final JsonObject cacheMe = new JsonObject().putString("cheese", "swiss");
        vertx.eventBus().send(InfinispanCacheVerticle.PUT, new JsonObject().putString("key", "item1").putObject("value", cacheMe), new Handler<Message<Object>>() {
            @Override
            public void handle(Message<Object> event) {
                vertx.eventBus().send(InfinispanCacheVerticle.GET, "item1", new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> event) {
                        VertxAssert.assertEquals(cacheMe, event.body());
                        VertxAssert.assertNotSame( cacheMe, event.body() );
                        VertxAssert.testComplete();
                    }
                });
            }
        });
    }
}
