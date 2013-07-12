package org.projectodd.vertx.infinispan;

import java.io.Serializable;

import org.vertx.java.core.json.JsonObject;

public class SerializableJson implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private String data;

    public SerializableJson(JsonObject obj) {
        this.data = obj.toString();
    }
    
    public JsonObject toObject() {
        return new JsonObject( this.data );
    }

}
