package com.psddev.dari.util.sa;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.asm.ClassReader;

/**
 * JVM that's used to run static analysis.
 */
public class Jvm {

    protected final Map<Method, List<JvmMethodListener>> listenersByMethod = new CompactMap<Method, List<JvmMethodListener>>();

    /**
     * Adds the given {@code listener} to the list of listeners that'd be
     * triggered when the given {@code method} is invoked.
     *
     * @param method Can't be {@code null}.
     * @param listener Can't be {@code null}.
     */
    public void addMethodListener(Method method, JvmMethodListener listener) {
        Preconditions.checkNotNull(method, "method");
        Preconditions.checkNotNull(listener, "listener");

        List<JvmMethodListener> listeners = listenersByMethod.get(method);

        if (listeners == null) {
            listeners = new ArrayList<JvmMethodListener>();
            listenersByMethod.put(method, listeners);
        }

        listeners.add(listener);
    }

    /**
     * Analyzes the given {@code objectClass}.
     *
     * @param objectClass Can't be {@code null}.
     */
    public void analyze(Class<?> objectClass) throws IOException {
        Preconditions.checkNotNull(objectClass, "objectClass");

        URL classUrl = objectClass.getResource("/" + objectClass.getName().replace('.', '/') + ".class");

        if (classUrl != null) {
            InputStream classInput = classUrl.openStream();

            try {
                new ClassReader(classInput).accept(
                        new JvmClassVisitor(this, objectClass),
                        ClassReader.SKIP_FRAMES);

            } finally {
                classInput.close();
            }
        }
    }
}
