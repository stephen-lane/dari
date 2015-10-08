package com.psddev.dari.util.sa;

import java.util.ArrayList;
import java.util.List;

import com.psddev.dari.util.asm.AnnotationVisitor;
import com.psddev.dari.util.asm.Attribute;
import com.psddev.dari.util.asm.ClassVisitor;
import com.psddev.dari.util.asm.FieldVisitor;
import com.psddev.dari.util.asm.MethodVisitor;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.asm.Type;

import com.psddev.dari.util.ObjectUtils;

class JvmClassVisitor extends ClassVisitor {

    private final Jvm jvm;
    private final Class<?> objectClass;

    public JvmClassVisitor(Jvm jvm, Class<?> objectClass) {
        super(Opcodes.ASM5);

        this.jvm = jvm;
        this.objectClass = objectClass;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        return null;
    }

    @Override
    public void visitAttribute(Attribute attr) {
    }

    @Override
    public void visitEnd() {
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
        return null;
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        List<Class<?>> argumentClasses = new ArrayList<Class<?>>();

        for (Type t : Type.getArgumentTypes(desc)) {
            argumentClasses.add(ObjectUtils.getClassByName(t.getClassName()));
        }

        try {
            return new JvmMethodVisitor(
                    access,
                    name,
                    desc,
                    signature,
                    exceptions,
                    jvm,
                    objectClass.getDeclaredMethod(name, argumentClasses.toArray(new Class<?>[0])));

        } catch (Throwable error) {
            return null;
        }
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
    }

    @Override
    public void visitSource(String source, String debug) {
    }
}
