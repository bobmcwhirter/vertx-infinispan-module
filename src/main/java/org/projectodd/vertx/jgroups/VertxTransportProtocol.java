package org.projectodd.vertx.jgroups;

import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.PhysicalAddress;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.vertx.java.core.Context;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.shareddata.Shareable;

public class VertxTransportProtocol extends TP implements Shareable {
    
    public static final AtomicInteger messageCounter = new AtomicInteger();

    static {
        ClassConfigurator.add((short) 8675, VertxAddress.class);
        ClassConfigurator.addProtocol((short) 8676, VertxTransportProtocol.class);
    }

    private Vertx vertx;
    private Context context;
    private String uuid;
    private String mcastGroup;

    private VertxAddress physicalAddress;

    public VertxTransportProtocol(Vertx vertx, String uuid, String mcastGroup) {
        this.vertx = vertx;
        this.context = vertx.currentContext();
        this.uuid = uuid;
        this.mcastGroup = mcastGroup;
        this.physicalAddress = new VertxAddress("org.jgroups.vertx.unicast." + this.uuid);
    }

    @Override
    public boolean supportsMulticasting() {
        return true;
    }

    @Override
    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        final byte[] buf = new byte[length];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = data[i + offset];
        }
        System.err.println(uuid + " * send multicast" );

        final JsonObject message = new JsonObject()
                .putNumber("message_number", messageCounter.getAndIncrement() )
                .putString("sender", getPhysicalAddress().toString())
                .putBinary("payload", buf);
        vertx.eventBus().publish("org.jgroups.vertx.multicast." + mcastGroup, message);
    }

    @Override
    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        final byte[] buf = new byte[length];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = data[i + offset];
        }
        final JsonObject message = new JsonObject()
                .putNumber("message_number", messageCounter.getAndIncrement() )
                .putString("sender", getPhysicalAddress().toString())
                .putBinary("payload", buf);
        System.err.println(uuid + " // " + local_addr + " * send unicast to: " + dest + " " + message );
        vertx.eventBus().send(dest.toString(), message);
    }

    @Override
    public String getInfo() {
        return "VertxTransportProtocol";
    }

    @Override
    protected PhysicalAddress getPhysicalAddress() {
        return this.physicalAddress;
    }

    public void receive(JsonObject body) {
        String sender = body.getString("sender");
        byte[] payload = body.getBinary("payload");
        System.err.println( uuid + " // " + local_addr + " * receive: " + body );
        VertxAddress addr = new VertxAddress(sender);
        receive(addr, payload, 0, payload.length);
    }

}
