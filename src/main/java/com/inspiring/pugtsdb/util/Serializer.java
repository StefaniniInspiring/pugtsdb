package com.inspiring.pugtsdb.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Serializer {

    private static final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(() -> createKryo());

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

    private static Kryo createKryo() {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
//        kryo.setRegistrationRequired(true);

        return kryo;
    }
}
