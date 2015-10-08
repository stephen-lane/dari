package com.psddev.dari.util.sa;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.util.List;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeDefinition;
import com.psddev.dari.util.asm.Type;

public class JvmObjectNew extends JvmObject {

    private JvmInvocation invocation;

    public JvmObjectNew(Type type) {
        super(type);
    }

    @Override
    public JvmInvocation addInvocation(AccessibleObject constructorOrMethod, List<JvmObject> arguments) {
        JvmInvocation invocation = super.addInvocation(constructorOrMethod, arguments);

        if (constructorOrMethod instanceof Constructor) {
            this.invocation = invocation;

            invocations.remove(invocation);
        }

        return invocation;
    }

    @Override
    protected Object doResolve() {
        Class<?> objectClass = ObjectUtils.getClassByName(type.getClassName());

        if (objectClass != null) {
            return TypeDefinition.getInstance(objectClass).newInstance();

        } else {
            return null;
        }
    }

    @Override
    public JvmObjectNew clone() {
        return updateClone(new JvmObjectNew(type));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append("new ");
        sb.append(typeToName(type));

        if (invocation != null) {
            invocation.appendTo(sb, ".");
        }
    }
}
