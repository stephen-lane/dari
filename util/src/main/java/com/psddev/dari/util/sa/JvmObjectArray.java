package com.psddev.dari.util.sa;

import java.util.Map;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.asm.Type;

public class JvmObjectArray extends JvmObject {

    private Map<JvmObject, JvmObject> values;

    public JvmObjectArray(Type type) {
        super(type);
    }

    public void store(JvmObject index, JvmObject value) {
        if (values == null) {
            values = new CompactMap<JvmObject, JvmObject>();
        }

        values.put(index, value);
    }

    @Override
    protected Object doResolve() {
        Map<Integer, Object> arrayMap = new CompactMap<Integer, Object>();
        int maxIndex = -1;

        if (values != null) {
            for (Map.Entry<JvmObject, JvmObject> entry : values.entrySet()) {
                int index = ObjectUtils.to(int.class, entry.getKey().resolve());
                Object value = entry.getValue().resolve();

                if (maxIndex < index) {
                    maxIndex = index;
                }

                arrayMap.put(index, value);
            }
        }

        Object[] array = new Object[maxIndex + 1];

        for (Map.Entry<Integer, Object> entry : arrayMap.entrySet()) {
            array[entry.getKey()] = entry.getValue();
        }

        return array;
    }

    @Override
    public JvmObjectArray clone() {
        JvmObjectArray clone = updateClone(new JvmObjectArray(type));

        if (values != null) {
            for (Map.Entry<JvmObject, JvmObject> entry : values.entrySet()) {
                clone.store(entry.getKey().clone(), entry.getValue().clone());
            }
        }

        return clone;
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append("{ ");

        if (values != null && !values.isEmpty()) {
            for (Map.Entry<JvmObject, JvmObject> entry : values.entrySet()) {
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(entry.getValue());
                sb.append(", ");
            }

            sb.setLength(sb.length() - 2);
            sb.append(' ');
        }

        sb.append('}');
    }
}
