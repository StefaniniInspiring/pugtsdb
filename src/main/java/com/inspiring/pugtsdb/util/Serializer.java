package com.inspiring.pugtsdb.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.inspiring.pugtsdb.repository.rocks.bean.MetaMetric;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

public class Serializer {

    private static final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(() -> getKryoSupplier().get());
    private static Supplier<Kryo> kryoSupplier = defaultKryoSupplier();

    public static byte[] serialize(Object object) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (Output output = new Output(stream)) {
                kryo.get().writeObject(output, object);
            }

            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> type) {
        try (Input input = new Input(bytes)) {
            return kryo.get().readObject(input, type);
        }
    }

    public static Supplier<Kryo> getKryoSupplier() {
        return kryoSupplier;
    }

    public static void setKryoSupplier(Supplier<Kryo> kryoSupplier) {
        Serializer.kryoSupplier = kryoSupplier;
    }

    public static Supplier<Kryo> defaultKryoSupplier() {
        return () -> {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            kryo.setRegistrationRequired(false);
            // Do not change the registration order below: https://github.com/EsotericSoftware/kryo#registration
            kryo.register(BigInteger.class, 20);
            kryo.register(BigDecimal.class, 21);
            kryo.register(ArrayList.class, 22);
            kryo.register(HashMap.class, 23);
            kryo.register(TreeMap.class, 24);
            kryo.register(HashSet.class, 25);
            kryo.register(TreeSet.class, 26);
            kryo.register(MetaMetric.class, 27);

            return kryo;
        };
    }
}
