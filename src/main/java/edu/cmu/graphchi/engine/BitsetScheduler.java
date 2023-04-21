package edu.cmu.graphchi.engine;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.Scheduler;

import java.util.BitSet;

/**
 * Scheduler implementation for "Selective Scheduling". Each vertex in the
 * graph has a bit which is 1 if the vertex should be updated, and 0 otherwise.
 * To obtain the current scheduler during computation, use context.getScheduler().
 * "选择性调度 "的调度器实现。图中的每个顶点
 * 图中的每个顶点都有一个位，如果该顶点应该被更新，则该位为1，否则为0。
 * 要在计算过程中获得当前的调度器，请使用context.getScheduler()。
 * @see edu.cmu.graphchi.GraphChiContext
 * @author akyrola
 */
public class BitsetScheduler implements Scheduler {

    private int nvertices;
    private BitSet bitset;
    private boolean hasNewTasks;

    public BitsetScheduler(int nvertices) {
        this.nvertices = nvertices;
        bitset = new BitSet(nvertices);
    }

    /**
     * Adds a vertex to schedule
     * 将一个顶点添加到时间表中
     * @param vertexId
     */
    @Override
    public void addTask(int vertexId) {
        bitset.set(vertexId, true);
        hasNewTasks = true;
    }

    /**
     * Removes vertices in an interval from schedule
     * 从时间表中删除一个区间的顶点
     * @param from first vertex to remove
     * @param to last vertex (inclusive)
     */
    @Override
    public void removeTasks(int from, int to) {
        for(int i=from; i<=to; i++) {
            bitset.set(i, false);
        }
    }

    /**
     * Adds all vertices in the graph to the schedule.
     * 将图中的所有顶点添加到时间表中。
     */
    @Override
    public void addAllTasks() {
        hasNewTasks = true;
        bitset.set(0, bitset.size(), true);
    }

    /**
     * Whether there are new tasks since previous reset().
     * 自上次reset()后是否有新任务。
     * @return
     */
    @Override
    public boolean hasTasks() {
        return hasNewTasks;
    }

    /**
     * Is vertex(i) scheduled or not.
     * @param i
     * @return
     */
    @Override
    public boolean isScheduled(int i) {
        return bitset.get(i);
    }

    /**
     * Sets all bits to zero/
     * 将所有位设置为零/
     */
    @Override
    public void removeAllTasks() {
        bitset.clear();
        hasNewTasks = false;
    }

    @Override
    /**
     * Convenience method for scheduling all out-neighbors of
     * a vertex.
     * 方便的方法，用于调度一个顶点的所有外邻接点。
     * @param vertex vertex in question
     */
    public void scheduleOutNeighbors(ChiVertex vertex) {
        int nEdges = vertex.numOutEdges();
        for(int i=0; i < nEdges; i++) {
            addTask(vertex.outEdge(i).getVertexId());
        }
    }

    @Override
    /**
     * Convenience method for scheduling all in-neighbors of
     * a vertex.
     * 方便的方法，用于调度一个顶点的所有内邻。
     * @param vertex vertex in question
     */
    public void scheduleInNeighbors(ChiVertex vertex) {
        int nEdges = vertex.numInEdges();
        for(int i=0; i < nEdges; i++) {
            addTask(vertex.inEdge(i).getVertexId());
        }
    }

    /**
     * Reset the "hasNewTasks" counter, but does not
     * clear the bits.
     * 重置 "hasNewTasks "计数器，但并没有 清除这些位。
     */
    public void reset() {
        hasNewTasks = false;
    }

}
