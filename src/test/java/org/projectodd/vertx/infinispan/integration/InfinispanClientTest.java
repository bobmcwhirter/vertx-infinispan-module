package org.projectodd.vertx.infinispan.integration;

import org.junit.Test;
import org.projectodd.vertx.infinispan.InfinispanClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

public class InfinispanClientTest extends TestVerticle {

    @Override
    public void start() {
        container.deployModule(System.getProperty("vertx.modulename"), new AsyncResultHandler<String>() {
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
    public void testNothing() {
        final JsonObject cacheMe = new JsonObject().putString("cheese", "swiss");
        
        final InfinispanClient infinispan = new InfinispanClient(vertx);
        
        infinispan.put("item1", cacheMe, new Handler<Void>() {
            @Override
            public void handle(Void event) {
                infinispan.get("item1", new Handler<JsonObject>() {
                    @Override
                    public void handle(JsonObject value) {
                        VertxAssert.assertEquals(cacheMe, value );
                        VertxAssert.assertNotSame( cacheMe, value );
                        VertxAssert.testComplete();
                    }
                });
            }
        });
    }
}
