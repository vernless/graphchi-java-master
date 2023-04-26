package edu.cmu.graphchi.preprocessing;

/**
 * Interface for objects that translate edge values from string
 * to the value type.
 * 用于将边值从字符串转换为数值类型的对象的接口
 * @param <ValueType>
 */
public interface EdgeProcessor <ValueType>  {

    public ValueType receiveEdge(int from, int to, String token);

}
