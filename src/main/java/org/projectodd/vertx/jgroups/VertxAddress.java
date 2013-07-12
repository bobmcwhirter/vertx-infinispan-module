package org.projectodd.vertx.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;

public class VertxAddress implements PhysicalAddress {

    private String address;
    
    public VertxAddress() {
    }

    public VertxAddress(String address) {
        this.address = address;
    }
    
    @Override
    public int size() {
        return this.address.getBytes().length;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeUTF( this.address );
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        this.address = in.readUTF();
    }

    @Override
    public int compareTo(Address o) {
        return this.address.compareTo( ((VertxAddress)o).address );
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(this.address);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.address = in.readUTF();
    }
    
    public String toString() {
        return this.address;
    }

}
