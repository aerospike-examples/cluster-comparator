package com.aerospike.comparator.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.AerospikeException;
import com.aerospike.comparator.dbaccess.AerospikeClientAccess;

public class InfoParser {
    
    public static class NodeData extends NamedData {
        public NodeData(String nodeName, Object data) {
            super(nodeName, data);
        }
    }
    private static interface NameFinder {
        String getName(Map<String, String> object);
    }

    public static class ObjectData extends NamedData {
        public ObjectData(Map<String, String> data, NameFinder nameFinder) {
            super(nameFinder.getName(data), data);
        }
    }
    
    private List<String> invokeCommandOnAllNodes(AerospikeClientAccess client, String command) {
        Map<String, String> results = client.invokeInfoCommandOnAllNodes(command);
        return new ArrayList<>(results.values());
    }
    
    private void parseDataParts(String[] dataParts, Map<String, String> results, boolean skipMetrics) {
        for (String thisPart : dataParts) {
            String[] keyValue = thisPart.split("=");
            String key = keyValue[0];
            if (key.contains("_") && skipMetrics) {
                // This is a metric
                continue;
            }
            String value = keyValue[1];
            results.put(key, value);
        }
        
    }
    private Map<String, String> parseInfoOutputToObject(String rawData, boolean skipMetrics) {
        Map<String, String> result = new HashMap<>();
        if (rawData != null && rawData.length() > 0) {
            String[] parts = rawData.split(";");
            parseDataParts(parts, result, skipMetrics);
        }
        return result;
    }
    
    private List<Map<String, String>> parseInfoOutputToObjectList(String rawData, boolean skipMetrics) {
        List<Map<String, String>> results = new ArrayList<>();
        if (rawData != null && rawData.length() > 0) {
            String[] records = rawData.split(";");
            for (String thisRecord : records) {
                Map<String, String> currentRecord = new HashMap<>();
                results.add(currentRecord);
                String[] parts = thisRecord.split(":");
                parseDataParts(parts, currentRecord, skipMetrics);
            }
        }
        return results;
    }

    private String invokeCommand(AerospikeClientAccess client, String command) {
        return client.invokeInfoCommandOnANode(command);
    }

    public Set<String> invokeCommandReturningSet(AerospikeClientAccess client, String command) {
        String rawData = invokeCommand(client, command);
        return new HashSet<String>(Arrays.asList(rawData.split(";")));
    }
    
    public Set<String> invokeCommandReturningSetOnAllNodes(AerospikeClientAccess client, String command) {
        List<String> rawData = invokeCommandOnAllNodes(client, command);
        Set<String> results = new HashSet<>();
        for (String thisData : rawData) {
            results.addAll(Arrays.asList(thisData.split(";")));
        }
        return results;
    }

    public List<Map<String, String>> invokeCommandReturningObjectList(AerospikeClientAccess client, String command) {
        return invokeCommandReturningObjectList(client,command, true);
    }
    public List<Map<String, String>> invokeCommandReturningObjectList(AerospikeClientAccess client, String command, boolean skipMetrics) {
        String rawData = invokeCommand(client, command);
        return parseInfoOutputToObjectList(rawData, skipMetrics);
    }
    
    public List<List<Map<String, String>>> invokeCommandReturningObjectListOnAllNodes(AerospikeClientAccess client, String command) {
        return invokeCommandReturningObjectListOnAllNodes(client, command, true);
    }
    public List<List<Map<String, String>>> invokeCommandReturningObjectListOnAllNodes(AerospikeClientAccess client, String command, boolean skipMetrics) {
        List<List<Map<String, String>>> results = new ArrayList<>();
        List<String> rawDatas = invokeCommandOnAllNodes(client, command);
        for (String rawData : rawDatas) {
            results.add(parseInfoOutputToObjectList(rawData, skipMetrics));
        }
        return results;
    }
    
    public List<Map<String, String>> invokeCommandReturningObjectOnAllNodes(AerospikeClientAccess client, String command) {
        return invokeCommandReturningObjectOnAllNodes(client, command, true);
    }
    public List<Map<String, String>> invokeCommandReturningObjectOnAllNodes(AerospikeClientAccess client, String command, boolean skipMetrics) {
        List<String> allNodesRawData = invokeCommandOnAllNodes(client, command);
        List<Map<String, String>> result = new ArrayList<>();
        for (String thisNodeData : allNodesRawData) {
            result.add(parseInfoOutputToObject(thisNodeData, skipMetrics));
        }
        return result;
    }

    public Map<String, String> invokeCommandReturningObject(AerospikeClientAccess client, String command) {
        return invokeCommandReturningObject(client, command, true);
    }
    public Map<String, String> invokeCommandReturningObject(AerospikeClientAccess client, String command, boolean skipMetrics) {
        String rawData = invokeCommand(client, command);
        return parseInfoOutputToObject(rawData, skipMetrics);
    }
    
    private Map<String, Field> getFieldsFromClass(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> namesToFields = new HashMap<>();
        for (Field thisField : fields) {
            FieldName annotation = thisField.getAnnotation(FieldName.class);
            String name;
            if (annotation != null) {
                name = annotation.value();
            }
            else {
                name = thisField.getName();
            }
            thisField.setAccessible(true);
            namesToFields.put(name, thisField);
        }
        return namesToFields;
    }
    private <T> void mapStringToField(String value, Field field, T obj) throws NumberFormatException, IllegalArgumentException, IllegalAccessException {
        Class<?> fieldType = field.getType();
        if (fieldType.equals(byte.class)) {
            field.setByte(obj, value == null || value.length() == 0 ? 0 : Byte.parseByte(value));
        }
        else if (fieldType.equals(Byte.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0 : Byte.parseByte(value));
        }
        else if (fieldType.equals(char.class)) {
            field.setChar(obj, value == null || value.length() == 0 ? 0 : Character.valueOf(value.charAt(0)));
        }
        else if (fieldType.equals(Character.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0 : Character.valueOf(value.charAt(0)));
        }
        else if (fieldType.equals(short.class)) {
            field.setShort(obj, value == null || value.length() == 0 ? 0 : Short.parseShort(value));
        }
        else if (fieldType.equals(Short.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0 : Short.parseShort(value));
        }
        else if (fieldType.equals(int.class)) {
            field.setInt(obj, value == null || value.length() == 0 ? 0 : Integer.parseInt(value));
        }
        else if (fieldType.equals(Integer.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0 : Integer.parseInt(value));
        }
        else if (fieldType.equals(long.class)) {
            field.setLong(obj, value == null || value.length() == 0 ? 0l : Long.parseLong(value));
        }
        else if (fieldType.equals(Long.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0l : Long.parseLong(value));
        }
        else if (fieldType.equals(float.class)) {
            field.setFloat(obj, value == null || value.length() == 0 ? 0.0f : Float.parseFloat(value));
        }
        else if (fieldType.equals(Float.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0.0f : Float.parseFloat(value));
        }
        else if (fieldType.equals(double.class)) {
            field.setDouble(obj, value == null || value.length() == 0 ? 0.0 : Double.parseDouble(value));
        }
        else if (fieldType.equals(Double.class)) {
            field.set(obj, value == null || value.length() == 0 ? 0.0 : Double.parseDouble(value));
        }
        else if (fieldType.equals(boolean.class)) {
            field.setBoolean(obj, value == null || value.length() == 0 ? false : Boolean.parseBoolean(value));
        }
        else if (fieldType.equals(Boolean.class)) {
            field.set(obj, value == null || value.length() == 0 ? false : Boolean.parseBoolean(value));
        }
        else if (fieldType.isEnum()) {
            Enum<?>[] constants = (Enum<?>[]) fieldType.getEnumConstants();
            String compareValue = value.toUpperCase();
            for (Enum<?> thisEnum : constants) {
                if (thisEnum.toString().toUpperCase().equals(compareValue)) {
                    field.set(obj, thisEnum);
                    break;
                }
            }
        }
        else {
            field.set(obj, value);
        }
    }
    
    private <T> List<T> parseObjectListStringsToObjectList(List<Map<String, String>> data, Class<T> clazz) {
        List<T> objects = new ArrayList<>();
        if (data.size() == 0) {
            return objects;
        }
        Constructor<T> constructor = null;
        try {
            constructor = clazz.getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw new AerospikeException("Class " + clazz.getSimpleName() + " does not have a no-arg constructor");
        }
        
        Map<String, Field> nameToFieldMapping = getFieldsFromClass(clazz);
        try {
            for (Map<String, String> thisResult : data) {
                T thisObj = constructor.newInstance();
                for (String thisKey : thisResult.keySet()) {
                    String value = thisResult.get(thisKey);
                    Field thisField = nameToFieldMapping.get(thisKey);
                    if (thisField != null) {
                        this.mapStringToField(value, thisField, thisObj);
                    }
                }
                objects.add(thisObj);
            }
        }
        catch (Exception e) {
            throw new AerospikeException(e);
        }
        return objects;
    }
    
    public <T> List<List<T>> invokeCommandOnAllNodes(AerospikeClientAccess client, String command, Class<T> clazz) {
        List<List<Map<String, String>>> results = this.invokeCommandReturningObjectListOnAllNodes(client, command, false);
        List<List<T>> objectList = new ArrayList<>();
        
        for (List<Map<String, String>> thisResult : results) {
            objectList.add(parseObjectListStringsToObjectList(thisResult, clazz));
        }
        return objectList;
    }
    
    public <T> List<T> invokeCommand(AerospikeClientAccess client, String command, Class<T> clazz) {
        List<Map<String, String>> results = this.invokeCommandReturningObjectList(client, command, false);
        return parseObjectListStringsToObjectList(results, clazz);
    }
}

