package com.psddev.dari.util.sa;

import com.psddev.dari.util.asm.Opcodes;

public class JvmObjectBinary extends JvmObject {

    private final JvmObject left;
    private final int operatorOpcode;
    private final JvmObject right;

    public JvmObjectBinary(JvmObject left, int operatorOpcode, JvmObject right) {
        super(left.type);

        this.left = left;
        this.operatorOpcode = operatorOpcode;
        this.right = right;

        triggerListeners(left, operatorOpcode, right, false);
        triggerListeners(right, operatorOpcode, left, true);
    }

    private static void triggerListeners(JvmObject left, int operatorOpcode, JvmObject right, boolean reverse) {
        if (left.listeners != null) {
            for (JvmObjectListener listener : left.listeners) {
                listener.onBinary(operatorOpcode, right, reverse);
            }
        }
    }

    @Override
    protected Object doResolve() {
        Object left = this.left.resolve();
        Object right = this.right.resolve();

        switch (operatorOpcode) {
            case Opcodes.IADD :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left + (Integer) right;
                }
                break;

            case Opcodes.LADD :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left + (Long) right;
                }
                break;

            case Opcodes.FADD :
                if (left instanceof Float && right instanceof Float) {
                    return (Float) left + (Float) right;
                }
                break;

            case Opcodes.DADD :
                if (left instanceof Double && right instanceof Double) {
                    return (Double) left + (Double) right;
                }
                break;

            case Opcodes.ISUB :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left - (Integer) right;
                }
                break;

            case Opcodes.LSUB :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left - (Long) right;
                }
                break;

            case Opcodes.FSUB :
                if (left instanceof Float && right instanceof Float) {
                    return (Float) left - (Float) right;
                }
                break;

            case Opcodes.DSUB :
                if (left instanceof Double && right instanceof Double) {
                    return (Double) left - (Double) right;
                }
                break;

            case Opcodes.IMUL :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left * (Integer) right;
                }
                break;

            case Opcodes.LMUL :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left * (Long) right;
                }
                break;

            case Opcodes.FMUL :
                if (left instanceof Float && right instanceof Float) {
                    return (Float) left * (Float) right;
                }
                break;

            case Opcodes.DMUL :
                if (left instanceof Double && right instanceof Double) {
                    return (Double) left * (Double) right;
                }
                break;

            case Opcodes.IDIV :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left / (Integer) right;
                }
                break;

            case Opcodes.LDIV :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left / (Long) right;
                }
                break;

            case Opcodes.FDIV :
                if (left instanceof Float && right instanceof Float) {
                    return (Float) left / (Float) right;
                }
                break;

            case Opcodes.DDIV :
                if (left instanceof Double && right instanceof Double) {
                    return (Double) left / (Double) right;
                }
                break;

            case Opcodes.IREM :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left % (Integer) right;
                }
                break;

            case Opcodes.LREM :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left % (Long) right;
                }
                break;

            case Opcodes.FREM :
                if (left instanceof Float && right instanceof Float) {
                    return (Float) left % (Float) right;
                }
                break;

            case Opcodes.DREM :
                if (left instanceof Double && right instanceof Double) {
                    return (Double) left % (Double) right;
                }
                break;

            case Opcodes.ISHL :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left << (Integer) right;
                }
                break;

            case Opcodes.LSHL :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left << (Long) right;
                }
                break;

            case Opcodes.ISHR :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left >> (Integer) right;
                }
                break;

            case Opcodes.LSHR :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left >> (Long) right;
                }
                break;

            case Opcodes.IUSHR :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left >>> (Integer) right;
                }
                break;

            case Opcodes.LUSHR :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left >>> (Long) right;
                }
                break;

            case Opcodes.IAND :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left & (Integer) right;
                }
                break;

            case Opcodes.LAND :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left & (Long) right;
                }
                break;

            case Opcodes.IOR :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left | (Integer) right;
                }
                break;

            case Opcodes.LOR :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left | (Long) right;
                }
                break;

            case Opcodes.IXOR :
                if (left instanceof Integer && right instanceof Integer) {
                    return (Integer) left ^ (Integer) right;
                }
                break;

            case Opcodes.LXOR :
                if (left instanceof Long && right instanceof Long) {
                    return (Long) left ^ (Long) right;
                }
                break;

            case Opcodes.LCMP :
            case Opcodes.FCMPL :
            case Opcodes.FCMPG :
            case Opcodes.DCMPL :
            case Opcodes.DCMPG :
                break;

            default :
                throw new IllegalArgumentException();
        }

        return null;
    }

    @Override
    public JvmObjectBinary clone() {
        return updateClone(new JvmObjectBinary(left.clone(), operatorOpcode, right.clone()));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(left);
        sb.append(' ');

        switch (operatorOpcode) {
            case Opcodes.IADD :
            case Opcodes.LADD :
            case Opcodes.FADD :
            case Opcodes.DADD :
                sb.append('+');
                break;

            case Opcodes.ISUB :
            case Opcodes.LSUB :
            case Opcodes.FSUB :
            case Opcodes.DSUB :
                sb.append('-');
                break;

            case Opcodes.IMUL :
            case Opcodes.LMUL :
            case Opcodes.FMUL :
            case Opcodes.DMUL :
                sb.append('*');
                break;

            case Opcodes.IDIV :
            case Opcodes.LDIV :
            case Opcodes.FDIV :
            case Opcodes.DDIV :
                sb.append('/');
                break;

            case Opcodes.IREM :
            case Opcodes.LREM :
            case Opcodes.FREM :
            case Opcodes.DREM :
                sb.append('%');
                break;

            case Opcodes.ISHL :
            case Opcodes.LSHL :
                sb.append("<<");
                break;

            case Opcodes.ISHR :
            case Opcodes.LSHR :
                sb.append(">>");
                break;

            case Opcodes.IUSHR :
            case Opcodes.LUSHR :
                sb.append(">>>");
                break;

            case Opcodes.IAND :
            case Opcodes.LAND :
                sb.append('&');
                break;

            case Opcodes.IOR :
            case Opcodes.LOR :
                sb.append('|');
                break;

            case Opcodes.IXOR :
            case Opcodes.LXOR :
                sb.append('^');
                break;

            case Opcodes.LCMP :
            case Opcodes.FCMPL :
            case Opcodes.FCMPG :
            case Opcodes.DCMPL :
            case Opcodes.DCMPG :
                sb.append("<=>");
                break;

            default :
                throw new IllegalArgumentException();
        }

        sb.append(' ');
        sb.append(right);
    }
}
