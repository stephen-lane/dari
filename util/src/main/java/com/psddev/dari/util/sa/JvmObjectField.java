package com.psddev.dari.util.sa;

import com.psddev.dari.util.asm.Type;

public class JvmObjectField extends JvmObject {

    private final Type owner;
    private final String fieldName;

    public JvmObjectField(Type type, Type owner, String fieldName) {
        super(type);

        this.owner = owner;
        this.fieldName = fieldName;
    }

    @Override
    protected Object doResolve() {
        return null;
    }

    @Override
    public JvmObjectField clone() {
        return updateClone(new JvmObjectField(type, owner, fieldName));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(typeToName(owner));
        sb.append('.');
        sb.append(fieldName);
    }
}
