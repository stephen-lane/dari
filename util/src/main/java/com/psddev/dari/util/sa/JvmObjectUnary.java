package com.psddev.dari.util.sa;

import com.psddev.dari.util.asm.Opcodes;

public class JvmObjectUnary extends JvmObject {

    private final int operatorOpcode;
    private final JvmObject object;

    public JvmObjectUnary(int operatorOpcode, JvmObject object) {
        super(object.type);

        this.operatorOpcode = operatorOpcode;
        this.object = object;
    }

    @Override
    protected Object doResolve() {
        Object object = this.resolve();

        switch (operatorOpcode) {
            case Opcodes.INEG :
                if (object instanceof Integer) {
                    return -((Integer) object);
                }
                break;

            case Opcodes.LNEG :
                if (object instanceof Long) {
                    return -((Long) object);
                }
                break;

            case Opcodes.FNEG :
                if (object instanceof Float) {
                    return -((Float) object);
                }
                break;

            case Opcodes.DNEG :
                if (object instanceof Double) {
                    return -((Double) object);
                }
                break;

            default :
                throw new IllegalArgumentException();
        }

        return null;
    }

    @Override
    public JvmObjectUnary clone() {
        return updateClone(new JvmObjectUnary(operatorOpcode, object.clone()));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        switch (operatorOpcode) {
            case Opcodes.INEG :
            case Opcodes.LNEG :
            case Opcodes.FNEG :
            case Opcodes.DNEG :
                sb.append('-');
                break;

            default :
                throw new IllegalArgumentException();
        }

        sb.append(object);
    }
}
