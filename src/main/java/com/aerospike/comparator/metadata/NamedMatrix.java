package com.aerospike.comparator.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NamedMatrix<T> {
    private Map<String, Map<String, T>> matrixData = new HashMap<>();
    
    public Map<String, T> getRawRowData(String rowName) {
        return this.matrixData.get(rowName);
    }
    public Set<NamedData> getRow(String rowName){
        Set<NamedData> result = new HashSet<>();
        Map<String, T> row = this.matrixData.get(rowName);
        if (row != null) {
            for (String key : row.keySet()) {
                T data = row.get(key);
                if (data != null) {
                    result.add(new NamedData(key, row.get(key)));
                }
            }
        }
        return result;
    }
    
    public Set<NamedData> getColumn(String colName) {
        Set<NamedData> result = new HashSet<>();
        for (String key : this.matrixData.keySet()) {
            Map<String, T> rowData = this.matrixData.get(key);
            T data = rowData.get(colName);
            if (data != null) {
                result.add(new NamedData(key, data));
            }
        }
        return result;
    }
    
    public NamedMatrix<T> transpose() {
        NamedMatrix<T> result = new NamedMatrix<T>();
        for (String rowName : matrixData.keySet()) {
            Map<String, T> row = matrixData.get(rowName);
            for (String colName: row.keySet()) {
                T data = row.get(colName);
                result.put(colName, rowName, data);
            }
        }
        return result;
    }
    
    public NamedMatrix<T> put(String rowName, String colName, T data) {
        Map<String, T> row = this.matrixData.get(rowName);
        if (row == null) {
            row = new HashMap<>();
            this.matrixData.put(rowName, row);
        }
        row.put(colName, data);
        
        return this;
    }
    
    public T get(String rowName, String colName) {
        Map<String, T> row = this.matrixData.get(rowName);
        if (row != null) {
            return row.get(colName);
        }
        return null;
    }
    
    public NamedMatrix<T> deleteRow(String rowName) {
        this.matrixData.remove(rowName);
        return this;
    }
    
    public NamedMatrix<T> deleteColumn(String colName) {
        for (String rowName : this.matrixData.keySet()) {
            this.matrixData.get(rowName).remove(colName);
        }
        return this;
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(1000).append('{');
        int rowSize = this.matrixData.size();
        int rowCount = 0;
        for (String rowName : this.matrixData.keySet()) {
            Map<String, T> row = this.matrixData.get(rowName);
            sb.append(rowName).append(":{");
            int colSize = row.size();
            int colCount = 0;
            for (String colName : row.keySet()) {
                sb.append(colName).append(':').append(row.get(colName));
                if (++colCount < colSize) {
                    sb.append(",");
                }
            }
            sb.append("}");
            if (++rowCount < rowSize) {
                sb.append(",");
            }
        }
        sb.append('}');
        return sb.toString();
    }
    
    public static void main(String[] args) {
        NamedMatrix<String> matrix = new NamedMatrix<String>();
        matrix.put("0", "0", "0-0");
        matrix.put("1", "0", "1-0");
        matrix.put("0", "1", "0-1");
        matrix.put("2", "2", "2-2");
        for (int i = 0; i < 7; i++) {
            matrix.put("3", ""+i, "3-" + i);
        }
        System.out.println("Original matrix: " + matrix.toString());
        System.out.println("Transposed matrix: " + matrix.transpose().toString());
        System.out.println("row0 = " + matrix.getRow("0"));
        System.out.println("col0 = " + matrix.getColumn("0"));
        matrix.deleteColumn("1");
        System.out.println("Matrix after removing column 1: " + matrix.toString());
        
    }
}