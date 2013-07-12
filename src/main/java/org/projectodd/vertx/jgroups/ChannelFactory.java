package org.projectodd.vertx.jgroups;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.RSVP;
import org.jgroups.protocols.UFC;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

public class ChannelFactory {

    private Container container;
    private Vertx vertx;
    private String uuid;

    public ChannelFactory(Container container, Vertx vertx, String uuid) {
        this.container = container;
        this.vertx = vertx;
        this.uuid = uuid;
    }

    public Channel newChannel() throws Exception {
        JChannel channel = new JChannel(getProtocols());
        return channel;
    }

    protected List<Protocol> getProtocols() throws Exception {
        List<Protocol> protocols = new ArrayList<>();

        protocols.add(createTransport());
        protocols.add( new TRACE( uuid ) );
        protocols.add(createPing());
        protocols.add(createMerge());
        protocols.add(createFailureDetection());
        protocols.add(createVerifySuspect());
        protocols.add(createBarrier());
        protocols.add(createNakAck());
        protocols.add(createUnicast());
        protocols.add(createStable());
        protocols.add(createGMS());
        protocols.add(createUFC());
        protocols.add(createMFC());
        protocols.add(createFragmentation());
        protocols.add(createRSVP());

        return protocols;
    }

    protected Protocol createTransport() throws UnknownHostException {

        /*
         * UDP udp = new UDP();
         * udp.setBindAddress( InetAddress.getByName( "localhost" ));
         * udp.setMulticastAddress(InetAddress.getByName("228.6.7.8"));
         * return udp;
         */


        VertxTransportProtocol vertxProto = new VertxTransportProtocol(this.vertx, uuid, "ISPN");
        vertx.sharedData().getMap("org.jgroups.vertx.proto").put( uuid, vertxProto  );
        
        container.deployWorkerVerticle(ReceiverWorkerVerticle.class.getName(), new JsonObject().putString("uuid", uuid), 5);
        container.deployVerticle(ReceiverVerticle.class.getName(), new JsonObject().putString("uuid", uuid).putString("mcast_group", "ISPN"));
        return vertxProto;

        /*
         * 
         * this.id = 1;
         * ip_mcast = true;
         * mcast_group_addr = InetAddress.getByName("228.6.7.8");
         * mcast_port = 46655;
         * tos = 8;
         * ucast_recv_buf_size = 20 * 1024 * 1024;
         * ucast_send_buf_size = 640 * 1024;
         * mcast_recv_buf_size = 25 * 1024 * 1024;
         * mcast_send_buf_size = 640 * 1024;
         * disable_loopback = false;
         * loopback = true;
         * max_bundle_size = 64 * 1024;
         * ip_ttl = 2;
         * enable_diagnostics = true;
         * bundler_type = "new";
         * 
         * thread_naming_pattern = "pl";
         * 
         * thread_pool_enabled = true;
         * thread_pool_min_threads = 2;
         * thread_pool_max_threads = 30;
         * thread_pool_keep_alive_time = 60000;
         * thread_pool_queue_enabled = true;
         * thread_pool_queue_max_size = 100;
         * thread_pool_rejection_policy = "Discard";
         * 
         * oob_thread_pool_enabled = true;
         * oob_thread_pool_min_threads = 2;
         * oob_thread_pool_max_threads = 30;
         * oob_thread_pool_keep_alive_time = 60000;
         * oob_thread_pool_queue_enabled = false;
         * oob_thread_pool_queue_max_size = 100;
         * oob_thread_pool_rejection_policy = "Discard";
         */
    }

    protected Protocol createPing() {
        PING ping = new PING();
        ping.setNumInitialMembers(2);
        ping.setTimeout(6000);
        return ping;
    }

    protected Protocol createMerge() {
        MERGE2 merge = new MERGE2();
        return merge;
    }

    protected Protocol createFailureDetection() {
        FD fd = new FD();
        return fd;

        /*
         * new FD_SOCK() {
         * {
         * this.id = 4;
         * 
         * }
         * },
         * new FD_ALL() {
         * {
         * this.id = 5;
         * setTimeout(15000);
         * }
         * },
         */
    }

    protected Protocol createVerifySuspect() {
        VERIFY_SUSPECT verify = new VERIFY_SUSPECT();
        return verify;

    }

    protected Protocol createBarrier() {
        BARRIER barrier = new BARRIER();
        return barrier;
    }

    protected Protocol createNakAck() {
        NAKACK2 nakAck = new NAKACK2();
        return nakAck;
        /*
         * this.id=8;
         * xmit_interval = 1000;
         * xmit_table_num_rows = 100;
         * xmit_table_msgs_per_row = 10000;
         * xmit_table_max_compaction_time = 10000;
         * max_msg_batch_size = 100;
         */
    }

    protected Protocol createUnicast() {
        UNICAST2 unicast = new UNICAST2();

        return unicast;
        /*
         * this.id=9;
         * stable_interval = 5000;
         * xmit_interval = 500;
         * max_bytes = 1024;
         * xmit_table_num_rows = 20;
         * xmit_table_msgs_per_row = 10000;
         * xmit_table_max_compaction_time = 10000;
         * max_msg_batch_size = 100;
         * conn_expiry_timeout = 0;
         */
    }

    protected Protocol createStable() {
        STABLE stable = new STABLE();
        return stable;
        /*
         * this.id=10;
         * stability_delay = 500;
         * desired_avg_gossip = 5000;
         * max_bytes = 1024;
         */
    }

    protected Protocol createGMS() {
        GMS gms = new GMS();
        return gms;
        /*
         * this.id=11;
         * setPrintLocalAddr(false);
         * setJoinTimeout(3000);
         * setViewBundling(true);
         */
    }

    protected Protocol createUFC() {
        UFC ufc = new UFC();
        return ufc;
        /*
         * this.id=12;
         * max_credits = 200000;
         * min_threshold = 0.20;
         */
    }

    protected Protocol createMFC() {
        MFC mfc = new MFC();
        return mfc;
        /*
         * this.id=13;
         * max_credits = 200000;
         * min_threshold = 0.20;
         */
    }

    protected Protocol createFragmentation() {
        FRAG2 frag = new FRAG2();
        return frag;
        /*
         * frag_size = 8000;
         */
    }

    protected Protocol createRSVP() {
        RSVP rsvp = new RSVP();
        return rsvp;
        /*
         * timeout = 60000;
         * resend_interval = 500;
         * ack_on_delivery = false;
         */
    }
}
