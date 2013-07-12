package org.projectodd.vertx.infinispan;

import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.jgroups.Channel;
import org.jgroups.View;
import org.jgroups.util.UUID;
import org.projectodd.vertx.jgroups.ChannelFactory;
import org.vertx.java.core.Context;
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
        try {
            Channel channel = configureChannel();
            System.err.println( "Cache thread: " + Thread.currentThread() );
            System.err.println("CHANNEL: " + channel);
            final Transport transport = new JGroupsTransport(channel) {
                @Override
                public void viewAccepted(View newView) {
                    System.err.println("VIEW: " + newView);
                    super.viewAccepted(newView);
                }
            };
            GlobalConfiguration cacheManagerConfig = GlobalConfigurationBuilder.defaultClusteredBuilder()
                    // .clusteredDefault()
                    .clusteredDefault().transport().transport(transport)
                    .globalJmxStatistics().disable().allowDuplicateDomains(true)
                    .build();
            // System.err.println( config );
            Configuration config = new ConfigurationBuilder()
                    .clustering()
                    .cacheMode(CacheMode.REPL_ASYNC)
                    .build();
            this.cacheManager = new DefaultCacheManager(cacheManagerConfig, config);
            this.cacheManager.start();
            this.cache = cacheManager.getCache();
            this.cache.start();
            vertx.eventBus().registerHandler(PUT, new Handler<Message<JsonObject>>() {
                @Override
                public void handle(final Message<JsonObject> putEvent) {
                    String key = putEvent.body().getString("key");
                    Object value = putEvent.body().getField("value");

                    if (value instanceof JsonObject) {
                        value = new SerializableJson((JsonObject) value);
                    }

                    final Context context = vertx.currentContext();

                    final NotifyingFuture<Object> putResult = InfinispanCacheVerticle.this.cache.putAsync(key, value);
                    putResult.attachListener(new FutureListener<Object>() {
                        @Override
                        public void futureDone(final java.util.concurrent.Future<Object> future) {
                            context.runOnContext(new Handler<Void>() {
                                @Override
                                public void handle(Void event) {
                                    putEvent.reply();
                                }
                            });
                        }
                    });
                }
            });
            vertx.eventBus().registerHandler(GET, new Handler<Message<String>>() {
                @Override
                public void handle(final Message<String> getEvent) {
                    NotifyingFuture<Object> result = InfinispanCacheVerticle.this.cache.getAsync(getEvent.body());
                    final Context context = vertx.currentContext();

                    result.attachListener(new FutureListener<Object>() {
                        @Override
                        public void futureDone(final java.util.concurrent.Future<Object> future) {
                            context.runOnContext(new Handler<Void>() {
                                @Override
                                public void handle(Void event) {
                                    System.err.println("members: " + transport.getMembers());
                                    try {
                                        Object value = future.get();
                                        if (value instanceof SerializableJson) {
                                            value = ((SerializableJson) value).toObject();
                                        }
                                        getEvent.reply(value);
                                    } catch (InterruptedException | ExecutionException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    });
                }
            });
            System.err.println("** members: " + transport.getMembers());
            startedResult.setResult(null);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private Channel configureChannel() throws Exception {
        return new ChannelFactory(this.container, this.vertx, UUID.randomUUID().toString() ).newChannel();
    }

    @Override
    public void stop() {
        this.cache.stop();
        this.cacheManager.stop();
        super.stop();
    }

}
