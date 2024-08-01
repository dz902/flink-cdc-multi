package org.example.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

// A TOOL TO TEST SERIALIZABILITY, WHICH SOMETIMES CAN BE PAINFUL TO DEBUG
// COPIED FROM FLINK CODE

public class SerializationTester {
    public static byte[] serializeObject(Object o) throws IOException {
        String version = System.getProperty("java.version");
        System.out.println(version);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Throwable var2 = null;

        Object var5;
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            Throwable var4 = null;

            System.out.println(o.toString());
            try {
                oos.writeObject(o);
                oos.flush();
                var5 = baos.toByteArray();
            } catch (Throwable var28) {
                System.out.println("HERE 28"+var28.toString());
                var5 = var28;
                var4 = var28;
                throw var28;
            } finally {
                if (oos != null) {
                    if (var4 != null) {
                        try {
                            oos.close();
                        } catch (Throwable var27) {
                            System.out.println("HERE 27"+var27.toString());
                            var4.addSuppressed(var27);
                        }
                    } else {
                        oos.close();
                    }
                }

            }
        } catch (Throwable var30) {
            System.out.println("HERE 30"+var30.toString());
            var2 = var30;
            throw var30;
        } finally {
            if (baos != null) {
                if (var2 != null) {
                    try {
                        baos.close();
                    } catch (Throwable var26) {
                        var2.addSuppressed(var26);
                    }
                } else {
                    baos.close();
                }
            }

        }

        return (byte[])var5;
    }
}
