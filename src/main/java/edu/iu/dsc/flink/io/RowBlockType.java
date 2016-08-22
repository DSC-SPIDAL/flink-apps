package edu.iu.dsc.flink.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

public class RowBlockType extends TypeInformation<RowBlock> {
    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 19;
    }

    @Override
    public int getTotalFields() {
        return 19;
    }

    @Override
    public Class<RowBlock> getTypeClass() {
        return RowBlock.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<RowBlock> createSerializer(
        ExecutionConfig config) {
        return new KryoSerializer<RowBlock>(RowBlock.class, config);
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RowBlock && obj.getClass() == RowBlock.class;
    }

    @Override
    public int hashCode() {
        return RowBlock.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof RowBlock;
    }
}
