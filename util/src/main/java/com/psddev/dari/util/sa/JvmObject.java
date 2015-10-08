package com.psddev.dari.util.sa;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.psddev.dari.util.asm.Type;

public abstract class JvmObject implements Cloneable {

    protected final Type type;
    protected List<JvmObjectListener> listeners;
    protected List<JvmInvocation> invocations;

    /**
     * Creates a new instance with the given {@code type}.
     *
     * @param type May be {@code null}.
     */
    protected JvmObject(Type type) {
        this.type = type;
    }

    /**
     * Returns {@code true} if this object would resolve to either
     * a {@code long} or a {@code double}.
     */
    public boolean isWide() {
        return type != null && type.getSize() == 2;
    }

    public void addListener(JvmObjectListener listener) {
        if (listeners == null) {
            listeners = new ArrayList<JvmObjectListener>();
        }

        listeners.add(listener);
    }

    public JvmInvocation addInvocation(AccessibleObject constructorOrMethod, List<JvmObject> arguments) {
        JvmInvocation invocation = new JvmInvocation(constructorOrMethod, arguments);
        boolean add = true;

        if (constructorOrMethod instanceof Method) {
            Method method = (Method) constructorOrMethod;
            Class<?> declaringClass = method.getDeclaringClass();

            if (declaringClass != null) {
                try {
                    for (PropertyDescriptor desc : Introspector.getBeanInfo(declaringClass).getPropertyDescriptors()) {
                        if (method.equals(desc.getReadMethod())) {
                            add = false;
                            break;
                        }
                    }

                } catch (Exception e) {
                    // Can't get getter information, but that's OK.
                }
            }
        }

        if (add) {
            if (invocations == null) {
                invocations = new ArrayList<JvmInvocation>();
            }

            invocations.add(invocation);
        }

        return invocation;
    }

    protected abstract Object doResolve();

    public final Object resolve() {
        Object resolved = doResolve();

        if (invocations != null) {
            for (JvmInvocation invocation : invocations) {
                invocation.resolve(resolved);
            }
        }

        return resolved;
    }

    protected String typeToName(Type type) {
        if (type == null) {
            return "this";

        } else {
            String className = type.getClassName();

            return className.startsWith("java.lang.") ? className.substring(10) : className;
        }
    }

    protected <T extends JvmObject> T updateClone(T clone) {
        if (invocations != null) {
            clone.invocations = new ArrayList<JvmInvocation>(invocations);
        }

        return clone;
    }

    @Override
    public abstract JvmObject clone();

    protected abstract void appendTo(StringBuilder sb);

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();

        appendTo(sb);

        if (invocations != null) {
            for (JvmInvocation i : invocations) {
                i.appendTo(sb, "->");
            }
        }

        return sb.toString();
    }
}
