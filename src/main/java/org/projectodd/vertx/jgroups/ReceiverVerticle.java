package org.projectodd.vertx.jgroups;

import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class ReceiverVerticle extends Verticle implements Handler<Message<JsonObject>> {

    private String uuid;
    private String mcastGroup;

    @Override
    public void start(Future<Void> startedResult) {
        System.err.println( "start receiver" );
        this.uuid = container.config().getString( "uuid" );
        this.mcastGroup = container.config().getString( "mcast_group" );
        
        System.err.println( "RECEIVER MCAST: " + mcastGroup );
        vertx.eventBus().registerHandler("org.jgroups.vertx.multicast." + mcastGroup, this );
        vertx.eventBus().registerHandler("org.jgroups.vertx.unicast." + uuid, this );
        
        startedResult.setResult(null);
        
        System.err.println( "started receiver" );
        
        System.err.println( "startedResult: " + startedResult.complete() );
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void handle(Message<JsonObject> event) {
        System.err.println( uuid + " BRIDGE" );
        vertx.eventBus().send("org.jgroups.vertx.unicast." + uuid + ".worker", event.body() );
    }

}
