package edu.cmu.graphchi.preprocessing;

/**
 * Converts a vertex-value from string to the value type.
 * 将一个顶点的值从字符串转换为值类型。
 */
public interface VertexProcessor  <ValueType> {

    ValueType receiveVertexValue(int vertexId, String token);

}
