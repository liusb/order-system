package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.type.*;

import java.util.Map;
import java.util.TreeMap;

public class Row {
    private TreeMap<Integer, Object> values;
    private int hashCode;

    public Row() {
        this.values = new TreeMap<Integer, Object>();
        this.hashCode = 0;
    }

    public void insert(int key, String strValue) {
        Object value;
        if (strValue.equals(ValueBoolean.booleanFalseValue))  {
            value = false;
        }else if(strValue.equals(ValueBoolean.booleanTrueValue)) {
            value = true;
        } else {
            value = parseValue(strValue);
        }
        this.values.put(key, value);
    }

    public void insert(int key, Long value) {
        this.values.put(key, value);
    }

    public Object parseValue(String s) {
        if (s.length() > 20) {
            return s;
        }
        char firstChar = s.charAt(0);
        if (firstChar == '+') {  // 会丢失加号, 直接不转换
//            return s;
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                return s;
            }
        }
        if (firstChar == '-' && s.length()>1) {
            firstChar = s.charAt(1);
        }
        if (firstChar >= '0' && firstChar <='9') {
            if (s.indexOf(' ') != -1) {
                return s;
            }
            if (s.indexOf('.') != -1) {
                if (s.lastIndexOf('0') == s.length()-1) {
                    return s;
                }
                try {
                    return Double.parseDouble(s);
                } catch (NumberFormatException e2) {
                    return s;
                }
            } else {
                try {
                    return Long.parseLong(s);
                } catch (NumberFormatException e) {
                    return s;
                }
            }
        }
        return s;
    }

    public void setHashCode(int hashCode) {
        this.hashCode = hashCode;
    }

    public int getHashCode() {
        return this.hashCode;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Object getValue(int columnId) {
        return values.get(columnId);
    }

    /**
     * 将数据序列化到byte数组中 格式
     * [hashCode(int), length(int), keyString(int,string), [key1(int),value(type,[value])], ...]
     * @param buffer 保证要足够大
     */
    public int writeToBytes(Data buffer) {
        Object value;
        buffer.reset();
        buffer.writeInt(hashCode);
        buffer.skip(4);
        buffer.writeString(((String) values.get(0)));
        values.remove(0);
        for (Map.Entry<Integer, Object> entry: values.entrySet()) {
            buffer.writeInt(entry.getKey());
            value = entry.getValue();
            if (value instanceof String) {
                buffer.writeByte(Value.STRING);
                buffer.writeString(((String) value));
            } else if (value instanceof Double) {
                buffer.writeByte(Value.DOUBLE);
                buffer.writeDouble((Double) value);
            } else if (value instanceof Long) {
                buffer.writeByte(Value.LONG);
                buffer.writeLong((Long) value);
            } else if (value instanceof Boolean) {
                if (((Boolean) value)) {
                    buffer.writeByte(Value.BOOLEAN_TRUE);
                } else {
                    buffer.writeByte(Value.BOOLEAN_FALSE);
                }
            }
        }
        buffer.setInt(4, buffer.getPos()-8);
        return buffer.getPos();
    }
}
