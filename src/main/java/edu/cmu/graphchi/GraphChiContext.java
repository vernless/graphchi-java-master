package edu.cmu.graphchi;

import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * GraphChiContext represents the current state of the computation.
 * This is passed to the update-function.
 * GraphChiContext表示计算的当前状态。
 * 这将被传递给更新函数。
 * @see edu.cmu.graphchi.GraphChiProgram
 */
public class GraphChiContext {


    public GraphChiContext() {}

    /**
     * @return Current interval of vertices being worked on.
     * 正在处理的顶点的当前区间。
     */
    public VertexInterval getCurInterval() {
        return curInterval;
    }

    protected void setCurInterval(VertexInterval curInterval) {
        this.curInterval = curInterval;
    }

    /**
     * @return the current iteration (starts from 0)
     * 当前迭代（从0开始）
     */
    public int getIteration() {
        return iteration;
    }

    protected void setIteration(int iteration) {
        this.iteration = iteration;
    }

    /**
     * @return  the total number of iterations
     * 迭代的总次数
     */
    public int getNumIterations() {
        return numIterations;
    }

    protected void setNumIterations(int numIterations) {
        this.numIterations = numIterations;
    }

    /**
     * Get the scheduler bound to this computation.
     * 获取与该计算绑定的调度器。
     * @return scheduler
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

    protected void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * @return the total number of edges in the current graph
     * 当前图中的边的总数
     */
    public long getNumEdges() {
        return numEdges;
    }

    protected void setNumEdges(long numEdges) {
        this.numEdges = numEdges;
    }

    /**
     * @return the total number of vertices in the current graph
     * 当前图形中顶点的总数量
     */
    public long getNumVertices() {
        return numVertices;
    }

    protected void setNumVertices(long numVertices) {
        this.numVertices = numVertices;
    }

    public int getThreadId() {
        return threadId;
    }

    public boolean isLastIteration() {
        return (this.iteration == this.getNumIterations() - 1);
    }

    public Object getThreadLocal() {
        return threadLocal;
    }

    /**
     * Advanced use - do not use.
     * @param threadLocal
     */
    public void setThreadLocal(Object threadLocal) {
        this.threadLocal = threadLocal;
    }

    public VertexIdTranslate getVertexIdTranslate() {
        return vertexIdTranslate;
    }

    protected void setVertexIdTranslate(VertexIdTranslate vertexIdTranslate) {
        this.vertexIdTranslate = vertexIdTranslate;
    }

    private Object threadLocal = null;

    private int threadId;
    private int iteration;
    private int numIterations;
    private long numEdges;
    private long numVertices;

    private Scheduler scheduler;
    private VertexInterval curInterval;
    private VertexIdTranslate vertexIdTranslate;

    public GraphChiContext clone(int _threadId) {
        GraphChiContext ctx = new GraphChiContext();
        ctx.threadId = _threadId;
        ctx.iteration = iteration;
        ctx.numIterations = numIterations;
        ctx.numEdges = numEdges;
        ctx.numVertices = numVertices;
        ctx.scheduler = scheduler;
        ctx.curInterval = curInterval;
        ctx.vertexIdTranslate = vertexIdTranslate;
        return ctx;
    }


}
