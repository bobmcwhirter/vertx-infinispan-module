package org.projectodd.vertx.jgroups;

import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class ReceiverWorkerVerticle extends Verticle {

    private String uuid;

    private VertxTransportProtocol proto;

    @Override
    public void start(Future<Void> startedResult) {

        this.uuid = container.config().getString("uuid");
        this.proto = (VertxTransportProtocol) vertx.sharedData().getMap("org.jgroups.vertx.proto").get(uuid);
        System.err.println("START WORKER: " + uuid );
        System.err.println( "PROTO: " + this.proto );

        this.vertx.eventBus().registerHandler("org.jgroups.vertx.unicast." + this.uuid + ".worker", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                System.err.println(uuid + " RECEIVE: " + event.body());
                proto.receive(event.body());
            }
        });

        startedResult.setResult(null);
    }

    @Override
    public void stop() {
        super.stop();
    }

}
