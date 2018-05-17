//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.neo4j.graphalgo.collections;

import java.util.Objects;

final class Intrinsics {
    private Intrinsics() {
    }

    public static <T> boolean isEmpty(Object value) {
        return value == null;
    }

    public static <T> T empty() {
        return (T) null;
    }

    public static <T> T cast(Object value) {
        return (T) value;
    }

    public static <T> T[] newArray(int arraySize) {
        return (T[]) null;
    }

    public static <T> boolean equals(Object e1, Object e2) {
        return Objects.equals(e1, e2);
    }

    public static <T> T add(T op1, T op2) {
        return (T) null;
    }
}
