package com.psddev.dari.util.sa;

import com.psddev.dari.util.asm.Type;

public class JvmObjectReturn extends JvmObject {

    private final JvmObject object;
    private final JvmInvocation invocation;

    public JvmObjectReturn(Type type, JvmObject object, JvmInvocation invocation) {
        super(type);

        this.object = object.clone();
        this.invocation = invocation;

        if (this.object.invocations != null) {
            this.object.invocations.remove(invocation);
        }
    }

    @Override
    protected Object doResolve() {
        return invocation.resolve(object.resolve());
    }

    @Override
    public JvmObjectReturn clone() {
        return updateClone(new JvmObjectReturn(type, object.clone(), invocation));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(object.toString());
        invocation.appendTo(sb, ".");
    }
}
