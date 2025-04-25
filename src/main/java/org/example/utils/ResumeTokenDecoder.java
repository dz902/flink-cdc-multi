package org.example.utils;

import org.bson.types.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResumeTokenDecoder {

    // CType constants
    private static final class CType {
        static final int kMinKey = 10;
        static final int kUndefined = 15;
        static final int kNullish = 20;
        static final int kNumeric = 30;
        static final int kStringLike = 60;
        static final int kObject = 70;
        static final int kArray = 80;
        static final int kBinData = 90;
        static final int kOID = 100;
        static final int kBool = 110;
        static final int kDate = 120;
        static final int kTimestamp = 130;
        static final int kRegEx = 140;
        static final int kDBRef = 150;
        static final int kCode = 160;
        static final int kCodeWithScope = 170;
        static final int kMaxKey = 240;

        static final int kNumericNaN = kNumeric + 0;
        static final int kNumericNegativeLargeMagnitude = kNumeric + 1;
        static final int kNumericNegative8ByteInt = kNumeric + 2;
        static final int kNumericNegative7ByteInt = kNumeric + 3;
        static final int kNumericNegative6ByteInt = kNumeric + 4;
        static final int kNumericNegative5ByteInt = kNumeric + 5;
        static final int kNumericNegative4ByteInt = kNumeric + 6;
        static final int kNumericNegative3ByteInt = kNumeric + 7;
        static final int kNumericNegative2ByteInt = kNumeric + 8;
        static final int kNumericNegative1ByteInt = kNumeric + 9;
        static final int kNumericNegativeSmallMagnitude = kNumeric + 10;
        static final int kNumericZero = kNumeric + 11;
        static final int kNumericPositiveSmallMagnitude = kNumeric + 12;
        static final int kNumericPositive1ByteInt = kNumeric + 13;
        static final int kNumericPositive2ByteInt = kNumeric + 14;
        static final int kNumericPositive3ByteInt = kNumeric + 15;
        static final int kNumericPositive4ByteInt = kNumeric + 16;
        static final int kNumericPositive5ByteInt = kNumeric + 17;
        static final int kNumericPositive6ByteInt = kNumeric + 18;
        static final int kNumericPositive7ByteInt = kNumeric + 19;
        static final int kNumericPositive8ByteInt = kNumeric + 20;
        static final int kNumericPositiveLargeMagnitude = kNumeric + 21;

        static final int kBoolFalse = kBool + 0;
        static final int kBoolTrue = kBool + 1;
    }

    private static final int kEnd = 4;
    private static final int kLess = 1;
    private static final int kGreater = 254;

    /**
     * Decodes a resume token string into a Map containing the token components
     *
     * @param input Resume token string to decode
     * @return Map containing the decoded resume token fields
     */
    public static Map<String, Object> decodeResumeToken(String input) {
        List<Object> bson = keystringToBson("v1", input);
        Map<String, Object> result = new HashMap<>();

        BSONTimestamp timestamp = (BSONTimestamp) bson.get(0);

        // Fix type casting issues by safely converting number types
        Integer version = toInteger(bson.get(1));

        Integer tokenType = null;
        Integer txnOpIndex;
        Boolean fromInvalidate = null;
        UUID uuid = null;
        Object identifierValue;
        String identifierKey;

        if (version >= 1) {
            tokenType = toInteger(bson.get(2));
            txnOpIndex = toInteger(bson.get(3));
            fromInvalidate = (Boolean) bson.get(4);
            uuid = maybeToUUID((Binary) bson.get(5));
            identifierValue = bson.get(6);
        } else {
            txnOpIndex = toInteger(bson.get(2));
            uuid = maybeToUUID((Binary) bson.get(3));
            identifierValue = bson.get(4);
        }

        identifierKey = version == 2 ? "eventIdentifier" : "documentKey";

        result.put("timestamp", timestamp);
        result.put("version", version);
        result.put("tokenType", tokenType);
        result.put("txnOpIndex", txnOpIndex);
        result.put("fromInvalidate", fromInvalidate);
        result.put("uuid", uuid);
        result.put(identifierKey, identifierValue);

        return result;
    }

    /**
     * Safely convert a number to Integer
     */
    private static Integer toInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Long) return ((Long) value).intValue();
        if (value instanceof Number) return ((Number) value).intValue();
        throw new ClassCastException("Cannot convert " + value.getClass().getName() + " to Integer");
    }

    /**
     * Converts a Binary to UUID if possible
     */
    private static UUID maybeToUUID(Binary data) {
        if (data != null && data.getType() == 4) {
            // UUID binary subtype is 4
            ByteBuffer buffer = ByteBuffer.wrap(data.getData());
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            return new UUID(mostSigBits, leastSigBits);
        }
        return null;
    }

    /**
     * Class to consume bytes from buffer
     */
    private static class BufferConsumer {
        private byte[] buf;
        private int index = 0;

        public BufferConsumer(byte[] buf) {
            this.buf = buf;
        }

        public Byte peekUint8() {
            if (index >= buf.length) return null;
            return buf[index];
        }

        public int readUint8() {
            if (index >= buf.length) {
                throw new IndexOutOfBoundsException("Unexpected end of input");
            }
            return buf[index++] & 0xFF; // Convert to unsigned
        }

        public long readUint32BE() {
            return ((long) readUint8() << 24) +
                ((long) readUint8() << 16) +
                ((long) readUint8() << 8) +
                readUint8();
        }

        public BigInteger readUint64BE() {
            BigInteger high = BigInteger.valueOf(readUint32BE());
            BigInteger low = BigInteger.valueOf(readUint32BE() & 0xFFFFFFFFL);
            return high.shiftLeft(32).or(low);
        }

        public byte[] readBytes(int n) {
            if (index + n > buf.length) {
                throw new IndexOutOfBoundsException("Unexpected end of input");
            }
            byte[] result = new byte[n];
            System.arraycopy(buf, index, result, 0, n);
            index += n;
            return result;
        }

        public String readCString() {
            int end = index;
            while (end < buf.length && buf[end] != 0) {
                end++;
            }

            String str = new String(buf, index, end - index, StandardCharsets.UTF_8);
            index = end + 1; // Skip the null terminator
            return str;
        }

        public String readCStringWithNuls() {
            StringBuilder result = new StringBuilder(readCString());

            while (peekUint8() != null && (peekUint8() & 0xFF) == 0xFF) {
                readUint8(); // Skip the 0xFF
                result.append('\0').append(readCString());
            }

            return result.toString();
        }
    }

    /**
     * Convert byte array to hex string
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x", b & 0xFF));
        }
        return hex.toString();
    }

    /**
     * Convert hex string to byte array
     */
    private static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Get number of bytes for an integer based on CType
     */
    private static int numBytesForInt(int ctype) {
        if (ctype >= CType.kNumericPositive1ByteInt) {
            return ctype - CType.kNumericPositive1ByteInt + 1;
        }
        return CType.kNumericNegative1ByteInt - ctype + 1;
    }

    /**
     * Read IEEE 754 double value from a 64-bit BigInteger
     */
    private static double readIEEE754Double(BigInteger val) {
        byte[] bytes = new byte[8];
        byte[] bigEndianBytes = val.toByteArray();

        // Copy bytes from the end of bigEndianBytes into our buffer
        int srcPos = Math.max(0, bigEndianBytes.length - 8);
        int length = Math.min(8, bigEndianBytes.length);
        int destPos = 8 - length;

        System.arraycopy(bigEndianBytes, srcPos, bytes, destPos, length);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.getDouble();
    }

    /**
     * Count leading zeros in a 64-bit value
     */
    private static int countLeadingZeros64(BigInteger num) {
        num = num.and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        BigInteger high = num.shiftRight(32);

        if (!high.equals(BigInteger.ZERO)) {
            return Integer.numberOfLeadingZeros(high.intValue());
        }

        return 32 + Integer.numberOfLeadingZeros(num.intValue());
    }

    /**
     * Read a value from the buffer based on its type
     */
    private static Object readValue(int ctype, String version, BufferConsumer buf) {
        boolean isNegative = false;

        switch (ctype) {
            case CType.kMinKey:
                return new MinKey();

            case CType.kMaxKey:
                return new MaxKey();

            case CType.kNullish:
                return null;

            case CType.kUndefined:
                return null; // Java doesn't have undefined, using null

            case CType.kBoolTrue:
                return Boolean.TRUE;

            case CType.kBoolFalse:
                return Boolean.FALSE;

            case CType.kDate:
                // XOR with 2^63 to convert from keystoring encoding
                BigInteger timestamp = buf.readUint64BE().xor(BigInteger.ONE.shiftLeft(63));
                return new Date(timestamp.longValue());

            case CType.kTimestamp: {
                int t = (int) buf.readUint32BE();
                int i = (int) buf.readUint32BE();
                return new BSONTimestamp(t, i);
            }

            case CType.kOID:
                return new ObjectId(bytesToHex(buf.readBytes(12)));

            case CType.kStringLike:
                return buf.readCStringWithNuls();

            case CType.kCode:
                // Return as string since Java driver's Code class might not be available
                return buf.readCStringWithNuls();

            case CType.kBinData: {
                int size = buf.readUint8();
                if (size == 0xFF) {
                    size = (int) buf.readUint32BE();
                }
                int subtype = buf.readUint8();
                return new Binary((byte) subtype, buf.readBytes(size));
            }

            case CType.kObject:
                return keystringToBsonPartial(version, buf, "named");

            case CType.kArray: {
                List<Object> arr = new ArrayList<>();
                while (buf.peekUint8() != null && buf.peekUint8() != 0) {
                    arr.add(keystringToBsonPartial(version, buf, "single"));
                }
                buf.readUint8(); // Skip end marker
                return arr;
            }

            case CType.kNumericNaN:
                return Double.NaN;

            case CType.kNumericZero:
                return 0;

            // Many numeric cases need careful handling
            case CType.kNumericNegative8ByteInt:
            case CType.kNumericNegative7ByteInt:
            case CType.kNumericNegative6ByteInt:
            case CType.kNumericNegative5ByteInt:
            case CType.kNumericNegative4ByteInt:
            case CType.kNumericNegative3ByteInt:
            case CType.kNumericNegative2ByteInt:
            case CType.kNumericNegative1ByteInt:
                isNegative = true;
                // Fall through to handle like positive integers but with sign flip

            case CType.kNumericPositive1ByteInt:
            case CType.kNumericPositive2ByteInt:
            case CType.kNumericPositive3ByteInt:
            case CType.kNumericPositive4ByteInt:
            case CType.kNumericPositive5ByteInt:
            case CType.kNumericPositive6ByteInt:
            case CType.kNumericPositive7ByteInt:
            case CType.kNumericPositive8ByteInt: {
                BigInteger encodedIntegerPart = BigInteger.ZERO;
                int intBytesRemaining = numBytesForInt(ctype);

                while (intBytesRemaining-- > 0) {
                    int b = buf.readUint8();
                    if (isNegative) {
                        b = (~b) & 0xFF;
                    }
                    encodedIntegerPart = encodedIntegerPart.shiftLeft(8).or(BigInteger.valueOf(b));
                }

                boolean haveFractionalPart = encodedIntegerPart.testBit(0);
                BigInteger integerPart = encodedIntegerPart.shiftRight(1);

                if (!haveFractionalPart) {
                    if (isNegative) {
                        integerPart = integerPart.negate();
                    }

                    // Return as long if possible, otherwise as BigInteger
                    if (integerPart.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0 &&
                        integerPart.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0) {
                        return integerPart.longValue();
                    }
                    return integerPart;
                }

                // Have fractional part, handling simplified for this implementation
                return Double.NaN; // Placeholder for complex decimal handling
            }

            case CType.kNumericNegativeLargeMagnitude:
                isNegative = true;
                // Fall through

            case CType.kNumericPositiveLargeMagnitude: {
                BigInteger encoded = buf.readUint64BE();
                if (isNegative) {
                    encoded = encoded.not().and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
                }

                if ("v0".equals(version)) {
                    return readIEEE754Double(encoded);
                }

                // Simplified large magnitude handling
                return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            }

            // Simplified handling for small magnitudes
            case CType.kNumericNegativeSmallMagnitude:
            case CType.kNumericPositiveSmallMagnitude:
                buf.readUint64BE(); // Skip reading
                return 0.0; // Simplified

            default:
                throw new IllegalArgumentException("Unknown keystring ctype: " + ctype);
        }
    }

    /**
     * Process the BSON data from the keystring buffer
     */
    private static Object keystringToBsonPartial(String version, BufferConsumer buf, String mode) {
        if ("named".equals(mode)) {
            Map<String, Object> contents = new HashMap<>();

            while (buf.peekUint8() != null) {
                int ctype = buf.readUint8();

                if (ctype == kLess || ctype == kGreater) {
                    ctype = buf.readUint8();
                }

                if (ctype == kEnd) {
                    break;
                }

                if (ctype == 0) {
                    break;
                }

                String key = buf.readCString();
                ctype = buf.readUint8();
                contents.put(key, readValue(ctype, version, buf));
            }

            return contents;
        } else if ("single".equals(mode)) {
            int ctype = buf.readUint8();

            if (ctype == kLess || ctype == kGreater) {
                ctype = buf.readUint8();
            }

            return readValue(ctype, version, buf);
        } else { // toplevel
            List<Object> contents = new ArrayList<>();

            while (buf.peekUint8() != null) {
                int ctype = buf.readUint8();

                if (ctype == kLess || ctype == kGreater) {
                    ctype = buf.readUint8();
                }

                if (ctype == kEnd) {
                    break;
                }

                contents.add(readValue(ctype, version, buf));
            }

            return contents;
        }
    }

    /**
     * Convert keystring to BSON objects
     */
    @SuppressWarnings("unchecked")
    private static List<Object> keystringToBson(String version, String buf) {
        byte[] bytes = hexToBytes(buf);
        Object result = keystringToBsonPartial(version, new BufferConsumer(bytes), "toplevel");
        return (List<Object>) result;
    }

    /**
     * Example usage
     */
    public static void main(String[] args) {
        // Example resume token
        String resumeTokenStr = "8263525400000000012900000100000000000000000000000000000000";
        Map<String, Object> decodedToken = decodeResumeToken(resumeTokenStr);

        System.out.println("Decoded Resume Token:");
        for (Map.Entry<String, Object> entry : decodedToken.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}