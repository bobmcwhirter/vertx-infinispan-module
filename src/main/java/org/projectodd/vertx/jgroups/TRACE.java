package org.projectodd.vertx.jgroups;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

public class TRACE extends Protocol {

    private static final Object writeLock = new Object();
    private String uuid;

    public TRACE(String uuid) {
        this.uuid = uuid;
    }

    public Object up(Event evt) {
        synchronized (writeLock) {
            System.out.println(uuid + " ---------------- TRACE (received) ----------------------");
            System.out.println(evt);
            System.out.println("--------------------------------------------------------");
        }
        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        synchronized (writeLock) {
            System.out.println(uuid + " ---------------- TRACE (received) ----------------------");
            System.out.println("message batch (" + batch.size() + " messages");
            System.out.println("--------------------------------------------------------");
        }
        up_prot.up(batch);
    }

    public Object down(Event evt) {
        synchronized (writeLock) {
            System.out.println(uuid + " ------------------- TRACE (sent) -----------------------");
            System.out.println(evt);
            System.out.println("--------------------------------------------------------");
        }
        return down_prot.down(evt);
    }

    public String toString() {
        return "Protocol TRACE";
    }

}
