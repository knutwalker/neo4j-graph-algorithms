package org.neo4j.graphalgo.core.utils;

import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

public final class ExceptionUtil {

    /**
     * @deprecated look at usage sites and replace with the proper replacements
     *             according to https://goo.gl/Ivn2kc
     */
    @Deprecated
    public static void throwUnchecked(final Throwable exception) {
        Exceptions.throwIfUnchecked(exception);
        throw new RuntimeException(exception);
    }

    public static <T> T throwKernelException(KernelException e) {
        Status status = e.status();
        String codeString = status.code().serialize();
        String message = e.getMessage();
        String newMessage;
        if (message == null || message.isEmpty()) {
            newMessage = codeString;
        } else {
            newMessage = codeString + ": " + message;
        }
        throw new RuntimeException(newMessage, e);
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
