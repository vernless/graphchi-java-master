package edu.cmu.graphchi.engine;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import edu.cmu.graphchi.hadoop.PigGraphChiBase;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.shards.MemoryShard;
import edu.cmu.graphchi.shards.SlidingShard;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

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
 * The engine responsible for executing a GraphChi computation.
 * @param <VertexDataType>  type of vertex-data
 * @param <EdgeDataType>   type of edge-data
 */
public class GraphChiEngine <VertexDataType, EdgeDataType> {

    protected String baseFilename; // 图路径
    protected int nShards;          // 分片
    protected ArrayList<VertexInterval> intervals; // 间隔
    protected ArrayList<SlidingShard<EdgeDataType>> slidingShards; // 滑动碎片

    protected BytesToValueConverter<EdgeDataType> edataConverter; // 边转换器
    protected BytesToValueConverter<VertexDataType> vertexDataConverter; //顶点转换器

//    GraphChiContext表示计算的当前状态。
// * 这将被传递给更新函数。
    protected GraphChiContextInternal chiContext = new GraphChiContextInternal();
    //管理使用ChiPointers访问的大块数据。 由GraphChi内部使用。
    private DataBlockManager blockManager;
    //并行执行器
    private ExecutorService parallelExecutor;
    //加载执行器
    private ExecutorService loadingExecutor;
    //度数处理程序
    private DegreeData degreeHandler;
    //GraphChi记录了每个顶点的度数（进出的边数）。顶点数据处理程序
    private VertexData<VertexDataType> vertexDataHandler;

    protected int subIntervalStart, subIntervalEnd; //区间开始 和 结束

    protected int maxWindow = 20000000;
    // 允许调度
    protected boolean enableScheduler = false;
    protected boolean onlyAdjacency = false;
    // 图中的每个顶点都有一个位，如果该顶点应该被更新，则该位为1，否则为0。
    protected BitsetScheduler scheduler = null;
    protected long nupdates = 0;
    // 启用确定性的执行
    protected boolean enableDeterministicExecution = true;
    private boolean useStaticWindowSize = false;
    // 内存预算
    protected long memBudget;
    // 顶点ID翻译
    protected VertexIdTranslate vertexIdTranslate;
    // 是否有顶点集合、边集合转换器
    protected boolean hasSetVertexDataConverter = false, hasSetEdgeDataConverter = false;


    private static final Logger logger = ChiLogger.getLogger("engine");

    /* Automatic loading of next window：自动加载下一个窗口 */
    private boolean autoLoadNext = false; // 仅适用于仅有邻接的情况!
    private boolean skipZeroDegreeVertices = false; // 跳过零度顶点

    private FutureTask<IntervalData> nextWindow;

    /* Metrics 度量衡：时间 */
    // 加载
    private final Timer loadTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "shard-loading", TimeUnit.SECONDS, TimeUnit.MINUTES);
    // 执行
    private final Timer executionTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "execute-updates", TimeUnit.SECONDS, TimeUnit.MINUTES);
    // Future 视为保存结果的对象–它可能暂时不保存结果，但将来会保存（一旦Callable 返回）。
    // Future 基本上是主线程可以跟踪进度以及其他线程的结果的一种方式。
    // 等待结果
    private final Timer waitForFutureTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "wait-for-future", TimeUnit.SECONDS, TimeUnit.MINUTES);
    // 初始化顶点
    private final Timer initVerticesTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "init-vertices", TimeUnit.SECONDS, TimeUnit.MINUTES);
    // 确定下一个窗口
    private final Timer determineNextWindowTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "det-next-window", TimeUnit.SECONDS, TimeUnit.MINUTES);

    // 修改内边/外边
    protected boolean modifiesInedges = true, modifiesOutedges = true;
    // 禁用内边/外边
    private boolean disableInEdges = false, disableOutEdges = false;


    /**
     * Constructor  构造函数
     * @param baseFilename input-file name
     * @param nShards number of shards
     * @throws FileNotFoundException
     * @throws IOException
     */
    public GraphChiEngine(String baseFilename, int nShards) throws FileNotFoundException, IOException {
        this.baseFilename = baseFilename;
        this.nShards = nShards;
        loadIntervals();
        blockManager = new DataBlockManager();
        degreeHandler = new DegreeData(baseFilename);

        // CA_test.txt.1.vtranslate 在fastSharder里面已经创建了
        File vertexIdTranslateFile = new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, nShards));
        if (vertexIdTranslateFile.exists()) {
            vertexIdTranslate = VertexIdTranslate.fromFile(vertexIdTranslateFile);
        } else {
            // 重写 VertexIdTranslate 里面的各种方法
            vertexIdTranslate = VertexIdTranslate.identity();
        }
        chiContext.setVertexIdTranslate(vertexIdTranslate);

        memBudget = Runtime.getRuntime().maxMemory() / 4;
        if (Runtime.getRuntime().maxMemory() < 256 * 1024 * 1024) {
            throw new IllegalArgumentException("Java Virtual Machine has only " + memBudget + "bytes maximum memory." +
                    " Please run the JVM with at least 256 megabytes of memory using -Xmx256m. For better performance, use higher value");
        }

    }

    /**
     * Access the intervals for shards.
     * 访问分片的区间。
     * @return
     */
    public ArrayList<VertexInterval> getIntervals() {
        return intervals;
    }

    // 加载区间
    protected void loadIntervals() throws FileNotFoundException, IOException {
        intervals = ChiFilenames.loadIntervals(baseFilename, nShards);
    }


    /**
     * Set the memorybudget in megabytes. Default is JVM's max memory / 4.
     * Memory budget affects the number of vertices loaded into memory at
     * any time.
     * 以兆字节为单位设置内存预算。默认值为 JVM 的最大内存 / 4。
     * 内存预算随时影响加载到内存中的顶点数。
     * @param mb
     */
    public void setMemoryBudgetMb(long mb) {
        memBudget = mb * 1024 * 1024;
    }

    /**
     * 获取当前的内存预算，单位为 bytes
     * @return the current memory budget in <b>bytes</b>.
     */
    public long getMemoryBudget() {
        return memBudget;
    }


    /**
     * You can instruct the engine to automatically ignore vertices that do not
     * have any edges. By default this is <b>false</b>.
     * 您可以指示引擎自动忽略没有任何边的顶点。默认情况下，这是错误的。
     * @param skipZeroDegreeVertices
     */
    public void setSkipZeroDegreeVertices(boolean skipZeroDegreeVertices) {
        this.skipZeroDegreeVertices = skipZeroDegreeVertices;
    }

    /**
     * @return the number of vertices in the current graph
     * 当前图形中顶点的数量
     */
    public int numVertices() {
        //return 1 + intervals.get(intervals.size() - 1).getLastVertex();
        return intervals.get(intervals.size() - 1).getLastVertex() + 1;
    }

    /**
     * For definition of "sliding shards", see http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi
     * 初始化滑动分片
     * @throws IOException
     */
    protected void initializeSlidingShards() throws IOException {
        slidingShards = new ArrayList<SlidingShard<EdgeDataType> >();
        for(int p=0; p < nShards; p++) {

            String edataFilename = (onlyAdjacency ? null : ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, p, nShards));

            // CA_test.txt.edata_java.0_1.adj
            String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, p, nShards);

            SlidingShard<EdgeDataType> slidingShard = new SlidingShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(p).getFirstVertex(),
                    intervals.get(p).getLastVertex());
            slidingShard.setConverter(edataConverter);
            slidingShard.setDataBlockManager(blockManager);
            slidingShard.setModifiesOutedges(modifiesOutedges);
            slidingShard.setOnlyAdjacency(onlyAdjacency);
            slidingShards.add(slidingShard);

        }
    }

    /**
     * For definition of "memory shards", see http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi
     * 对于“内存分片”的定义
     * @throws IOException
     */
    protected MemoryShard<EdgeDataType> createMemoryShard(int intervalStart, int intervalEnd, int execInterval) {
        // CA_test.txt.edata_java.e4B.0_1 不存在
        String edataFilename = (onlyAdjacency ? null : ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, execInterval, nShards));
        File edataFile = new File(edataFilename);
        if(edataFile.exists()){
            // 不存在
            System.out.println("edataFile is exist");
        }
        // 边文件：CA_test.txt.edata_java.0_1.adj
        String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, execInterval, nShards);
        MemoryShard<EdgeDataType> newMemoryShard = new MemoryShard<EdgeDataType>(edataFilename, adjFilename,
                intervals.get(execInterval).getFirstVertex(),
                intervals.get(execInterval).getLastVertex());
        newMemoryShard.setConverter(edataConverter);
        newMemoryShard.setDataBlockManager(blockManager);
        newMemoryShard.setOnlyAdjacency(onlyAdjacency);
        return newMemoryShard;
    }


    /**
     * Runs the GraphChi program for given number of iterations. <b>Note:</b> Prior to calling this,
     * you must have set the edge-data and vertex-data converters:
     * 运行给定迭代次数的 GraphChi 程序。注意：在调用此函数之前，必须设置边数据和顶点数据转换器：
     *   setEdataConverter()
     *   setVertexDataConverter()
     * @param program yoru GraphChi program
     * @param niters number of iterations
     * @throws IOException
     */
    public void run(GraphChiProgram<VertexDataType, EdgeDataType> program, int niters) throws IOException {

        if (!hasSetEdgeDataConverter) {
            throw new IllegalStateException("You need to call setEdataConverter() prior to calling run()!");
        }
        if (!hasSetVertexDataConverter) {
            throw new IllegalStateException("You need to call setVertexDataConverter() prior to calling run()!");
        }

        int nprocs = 4;
        if (Runtime.getRuntime().availableProcessors() > nprocs) {
            nprocs = Runtime.getRuntime().availableProcessors();
        }

        if (System.getProperty("num_threads") != null) {
            nprocs = Integer.parseInt(System.getProperty("num_threads"));
        }

        logger.info(":::::::: Using " + nprocs + " execution threads :::::::::");

        parallelExecutor = Executors.newFixedThreadPool(nprocs);
        loadingExecutor = Executors.newFixedThreadPool(4);

        chiContext.setNumIterations(niters);

        long startTime = System.currentTimeMillis();
        initializeSlidingShards();

        // false 默认为false
        if (enableScheduler) {
            initializeScheduler();
            chiContext.setScheduler(scheduler);
            scheduler.addAllTasks();
            logger.info("Using scheduler!");
        }  else {
            // 设置模拟调度程序
            chiContext.setScheduler(new MockScheduler());
        }

        // 下面两个的条件都是false
        if (disableInEdges) {
            ChiVertex.disableInedges = true;
        }
        if (disableOutEdges) {
            ChiVertex.disableOutedges = true;
        }

        /* Initialize vertex-data handler
        * 初始化顶点数据处理程序 */
        if (vertexDataConverter != null) {
            vertexDataHandler = new VertexData<VertexDataType>(numVertices(), baseFilename, vertexDataConverter, true);
            vertexDataHandler.setBlockManager(blockManager);
        }

        chiContext.setNumEdges(numEdges());


        // 循环次数
        for(int iter=0; iter < niters; iter++) {
            /* Wait for executor have finished all writes
            * 等待执行者完成所有写入工作 */
            while (!blockManager.empty()) {
                try {
                    System.out.println("go to sleep");
                    Thread.sleep(50);
                } catch (InterruptedException ie) {}
            }
            blockManager.reset();
            // Iteration == 0 时，是初始化每个顶点值
            chiContext.setIteration(iter);
            chiContext.setNumVertices(numVertices());
            // PageRank除了 update方法外，其他的方法都无意义
            program.beginIteration(chiContext);

            // scheduler is null
            if (scheduler != null) {
                if (iter > 0 && !scheduler.hasTasks()) {
                    logger.info("No new tasks to run. Terminating.");
                    break;
                }
                scheduler.reset();
            }

            for(int execInterval=0; execInterval < nShards; ++execInterval) {
                int intervalSt = intervals.get(execInterval).getFirstVertex();
                int intervalEn = intervals.get(execInterval).getLastVertex();
                // 开始输出
                logger.info("start------------------------------------------------------------------");
                logger.info((System.currentTimeMillis() - startTime) * 0.001 + "s: iteration: " + iter + ", interval: " + intervalSt + " -- " + intervalEn);

                // false
                if (program instanceof PigGraphChiBase) {
                    ((PigGraphChiBase) program).setStatusString("GraphChi iteration " + iter + " / " + (niters - 1) + ";" +
                            "  vertex interval:" + intervalSt + " -- " + intervalEn);

                }
                // PageRank除了 update方法外，其他的方法都无意义
                program.beginInterval(chiContext, intervals.get(execInterval));

                MemoryShard<EdgeDataType> memoryShard = null;

                // 下面可以执行
                if (!disableInEdges) {
                    if (!onlyAdjacency || !autoLoadNext || nextWindow == null) {

                        if (!disableOutEdges) {
                            slidingShards.get(execInterval).flush();     // MESSY!
                        }
                        memoryShard = createMemoryShard(intervalSt, intervalEn, execInterval);
                    } else {
                        memoryShard = null;
                    }
                }
                // 开始顶点：intervalSt
                subIntervalStart = intervalSt;
                /// 结束顶点：intervalEn
                while (subIntervalStart <= intervalEn) {
                    int adjMaxWindow = maxWindow;
                    // false
                    if (Integer.MAX_VALUE - subIntervalStart < maxWindow) {
                        adjMaxWindow = Integer.MAX_VALUE - subIntervalStart - 1;
                    }

                    if (anyVertexScheduled(subIntervalStart, Math.min(intervalEn, subIntervalStart + adjMaxWindow ))) {
                        // 顶点数组
                        ChiVertex<VertexDataType, EdgeDataType>[] vertices = null;
                        int vertexBlockId = -1;

                        if (!autoLoadNext || nextWindow == null) {
                            try {
                                subIntervalEnd = determineNextWindow(subIntervalStart, Math.min(intervalEn, subIntervalStart + adjMaxWindow ));
                            } catch (NoEdgesInIntervalException nie) {
                                logger.info("No edges, skip: " + subIntervalStart + " -- " + subIntervalEnd);
                                subIntervalEnd = subIntervalStart + adjMaxWindow;
                                subIntervalStart = subIntervalEnd + 1;
                                continue;
                            }
                            int nvertices = subIntervalEnd - subIntervalStart + 1;

                            logger.info("Subinterval:: " + subIntervalStart + " -- " + subIntervalEnd + " (iteration " + iter + ")");
                            vertices = new ChiVertex[nvertices];

                            // 初始化顶点，设置顶点在RandomAccessFile 中的位置指针
                            logger.info("Init vertices...");
                            vertexBlockId = initVertices(nvertices, subIntervalStart, vertices);

                            logger.info("Loading...");
                            long t0 = System.currentTimeMillis();
                            loadBeforeUpdates(execInterval, vertices, memoryShard, subIntervalStart, subIntervalEnd);
                            logger.info("Load took: " + (System.currentTimeMillis() - t0) + "ms");
                        } else {// 不执行
                            /* This is a mess! */
                            try {
                                long tf = System.currentTimeMillis();
                                final TimerContext _timer = waitForFutureTimer.time();
                                IntervalData next = nextWindow.get();

                                memoryShard = next.getMemShard();
                                _timer.stop();
                                logger.info("Waiting for future task loading took " + (System.currentTimeMillis() - tf) + " ms");
                                if (subIntervalStart != next.getSubInterval().getFirstVertex()) {
                                    throw new IllegalStateException("Future loaders interval does not match the expected one! " +
                                            subIntervalStart + " != " + next.getSubInterval().getFirstVertex());
                                }
                                subIntervalEnd = next.getSubInterval().getLastVertex();
                                vertexBlockId = next.getVertexBlockId();
                                vertices = next.getVertices();
                                nextWindow = null;
                            } catch (Exception err) {
                                throw new RuntimeException(err);
                            }
                        }
                        // 不执行
                        if (autoLoadNext) {
                            /* Start a future for loading the next window */
                            adjMaxWindow = maxWindow;
                            if (Integer.MAX_VALUE - subIntervalEnd < maxWindow) {
                                adjMaxWindow = Integer.MAX_VALUE - subIntervalEnd - 1;
                            }

                            if (subIntervalEnd + 1 <= intervalEn) {
                                nextWindow = new FutureTask<IntervalData>(new AutoLoaderTask(new VertexInterval(subIntervalEnd + 1,
                                        Math.min(intervalEn, subIntervalEnd + 1 + adjMaxWindow)), execInterval, memoryShard));
                            } else if (execInterval < nShards - 1) {
                                int nextIntervalSt = intervals.get(execInterval + 1).getFirstVertex();
                                int nextIntervalEn = intervals.get(execInterval + 1).getLastVertex();

                                slidingShards.get(execInterval).setOffset(memoryShard.getStreamingOffset(),
                                        memoryShard.getStreamingOffsetVid(), memoryShard.getStreamingOffsetEdgePtr());
                                nextWindow = new FutureTask<IntervalData>(new AutoLoaderTask(new VertexInterval(nextIntervalSt,
                                        Math.min(nextIntervalEn, nextIntervalSt + 1 + adjMaxWindow)), execInterval + 1,
                                        createMemoryShard(nextIntervalSt, nextIntervalEn, execInterval + 1)));

                            }
                            if (nextWindow != null) {
                                loadingExecutor.submit(nextWindow);
                            }

                        }
                        /* Clear scheduler bits */
                        // scheduler is null
                        if (scheduler != null) {
                            // scheduler is null
                            scheduler.removeTasks(subIntervalStart, subIntervalEnd);
                        }

                        // 无意义
                        chiContext.setCurInterval(new VertexInterval(subIntervalStart, subIntervalEnd));

                        // PageRank除了 update方法外，其他的方法都无意义
                        program.beginSubInterval(chiContext, new VertexInterval(subIntervalStart, subIntervalEnd));

                        long t1 = System.currentTimeMillis();
                        // 修改 DataBlockManager 中的数据
                        execUpdates(program, vertices);
                        logger.info("Update exec: " + (System.currentTimeMillis() - t1) + " ms.");

                        // Write vertices (async) 写下顶点
                        final int _firstVertex = subIntervalStart;
                        // 顶点的blockId = 0
                        final int _blockId = vertexBlockId;
                        parallelExecutor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    // 写入顶点到内存中的RandomAccessFile，并把blockManager中的顶点数据清空
                                    vertexDataHandler.releaseAndCommit(_firstVertex, _blockId);
                                } catch (IOException ioe) {
                                    ioe.printStackTrace();
                                }
                            }
                        });

                        subIntervalStart = subIntervalEnd + 1;
                        // PageRank除了 update方法外，其他的方法都无意义
                        program.endSubInterval(chiContext, new VertexInterval(subIntervalStart, subIntervalEnd));

                    }  else {
                        subIntervalEnd = subIntervalStart + adjMaxWindow;
                        logger.info("Skipped interval - no vertices scheduled. " + subIntervalStart + " -- " + subIntervalEnd);

                        subIntervalStart = subIntervalEnd + 1;
                    }
                }


                /* Commit */
                // 写出边，并把blockManager中的全部数据——边数据，因为
                // vertexDataHandler.releaseAndCommit(_firstVertex, _blockId);
                // 清空了顶点数据
                if (!disableInEdges) {
                    // 写入外存的..blockdir..文件中
                    //  false 和 true 只能修改出边
                    memoryShard.commitAndRelease(modifiesInedges, modifiesOutedges);
                    if (!disableOutEdges && !autoLoadNext) {
                        slidingShards.get(execInterval).setOffset(memoryShard.getStreamingOffset(),
                                memoryShard.getStreamingOffsetVid(), memoryShard.getStreamingOffsetEdgePtr());
                    }
                }
            }

            for(SlidingShard shard : slidingShards) {
                shard.flush();
                shard.setOffset(0, 0, 0);
            }
            // PageRank除了 update方法外，其他的方法都无意义
            program.endIteration(chiContext);
        }    // Iterations

        parallelExecutor.shutdown();
        loadingExecutor.shutdown();

        if (vertexDataHandler != null) {
            vertexDataHandler.close();
        }
        logger.info("Engine finished in: " + (System.currentTimeMillis() - startTime) * 0.001 + " secs.");
        logger.info("Updates: " + nupdates);
    }

    // 任何顶点计划
    private boolean anyVertexScheduled(int subIntervalStart, int lastVertex) {
        if (!enableScheduler) {
            return true;
        }

        for(int i=subIntervalStart; i<= lastVertex; i++) {
            if (scheduler.isScheduled(i)) {
                return true;
            }
        }
        return false;
    }

    // 初始化调度程序
    private void initializeScheduler() {
        scheduler = new BitsetScheduler(numVertices());
    }

    public static volatile int indexx = 0;
    // 执行更新
    private void execUpdates(final GraphChiProgram<VertexDataType, EdgeDataType> program,
                             final ChiVertex<VertexDataType, EdgeDataType>[] vertices) {
        if (vertices == null || vertices.length == 0) {
            return;
        }
        TimerContext _timer = executionTimer.time();
        // 下面条件为false
        if (Runtime.getRuntime().availableProcessors() == 1) {
            /* Sequential updates */
            // 顺序更新
            for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                if (vertex != null) {
                    nupdates++;
                    program.update(vertex, chiContext);
                }
            }
        } else {
            final Object termlock = new Object();
            final int chunkSize = 1 + vertices.length / 64;
            final int nWorkers = vertices.length / chunkSize + 1;
            //System.out.println("nWorker:" + nWorkers);
            final AtomicInteger countDown = new AtomicInteger(1 + nWorkers);
           // System.out.println("countD:" + countDown);
//          enableDeterministicExecution = true,不执行下面语句
            if (!enableDeterministicExecution) {
                for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                    if (vertex != null) {
                        vertex.parallelSafe = true;
                    }
                }
            }

            /* Parallel updates. One thread for non-parallel safe updates, others
     updated in parallel. This guarantees deterministic execution.
            并行更新。一个线程用于非并行 安全更新，其他线程则是并行更新。这保证了确定性的执行。
      */

            /* Non-safe updates  非并行 安全更新  对每个顶点进行更新*/
            parallelExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    int thrupdates = 0;
                    // 0
                    GraphChiContext threadContext = chiContext.clone(0);
                    try {
                        for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                            // true
                            //System.out.println("vertex" + vertex.getId() + ":"+vertex.parallelSafe);
                            if (vertex != null && !vertex.parallelSafe) {
                                //System.out.println("no1:" + thrupdates + " -- " + threadContext.getIteration());
                                thrupdates++;
                                program.update(vertex, threadContext);
                            }
                        }
                        //System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }  finally {
                        int pending = countDown.decrementAndGet();
                        synchronized (termlock) {
                            //System.out.println("index---0:" + indexx++ + "  nupdates:" + nupdates  + " pending:" + pending);
                            nupdates += thrupdates;
                            if (pending == 0) {
                                termlock.notifyAll();;
                            }
                        }
                    }
                }
            });

            //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            /* Parallel updates 并行更新 : 从第二次开始*/
            for(int thrId = 0; thrId < nWorkers; thrId++) {

                final int myId = thrId;
                final int chunkStart = myId * chunkSize;

                final int chunkEnd = chunkStart + chunkSize;

                parallelExecutor.submit(new Runnable() {

                    @Override
                    public void run() {
                        int thrupdates = 0;
                        // 从1开始，即可以进行更新
                        GraphChiContext threadContext = chiContext.clone(1 + myId);

                        try {
                            int end = chunkEnd;
                            if (end > vertices.length) {
                                end = vertices.length;
                            }
                            for(int i = chunkStart; i < end; i++) {
                                ChiVertex<VertexDataType, EdgeDataType> vertex = vertices[i];
                                if (vertex != null && vertex.parallelSafe) {
                                    thrupdates++;
                                    //System.out.println("no2:" + thrupdates + " -- " + threadContext.getIteration() + "vertex:" + vertex.getId());
                                    program.update(vertex, threadContext);
                                }
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            int pending = countDown.decrementAndGet();
                            synchronized (termlock) {
                                long p1 =nupdates;
                                nupdates += thrupdates;

                                //System.out.println("index+++1:" + indexx++ + "  nupdates:" + nupdates + " pending:" + pending);
                                if (pending == 0) {
                                    termlock.notifyAll();
                                }
                            }
                        }
                    }
                });
            }
            synchronized (termlock) {
                while(countDown.get() > 0) {
                    try {
                        termlock.wait(1500);
                    } catch (InterruptedException e) {
                        // What to do?
                        e.printStackTrace();
                    }
                    if (countDown.get() > 0) {
                        logger.info("Waiting for execution to finish: countDown:" + countDown.get());
                    }
                }
            }

        }
        _timer.stop();
    }

    // 初始化顶点
    protected int initVertices(int nvertices, int firstVertexId, ChiVertex<VertexDataType, EdgeDataType>[] vertices) throws IOException
    {
        final TimerContext _timer = initVerticesTimer.time();
        ChiVertex.edgeValueConverter = edataConverter;
        ChiVertex.vertexValueConverter = vertexDataConverter;
        ChiVertex.blockManager = blockManager;

        // 顶点blockId为0，边blockId为1
        int blockId = (vertexDataConverter != null ? vertexDataHandler.load(firstVertexId, firstVertexId + nvertices - 1) : -1);
        for(int j=0; j < nvertices; j++) {
            // false
            if (enableScheduler && !scheduler.isScheduled(j + firstVertexId)) {
                continue;
            }

            VertexDegree degree = degreeHandler.getDegree(j + firstVertexId);

            // false
            if (skipZeroDegreeVertices && (degree.inDegree + degree.outDegree == 0)) {
                continue;
            }

            ChiVertex<VertexDataType, EdgeDataType> v = new ChiVertex<VertexDataType, EdgeDataType>(j + firstVertexId, degree);

            // 设置指针
            if (vertexDataConverter != null) {
                v.setDataPtr(vertexDataHandler.getVertexValuePtr(j + firstVertexId, blockId));
            }
            vertices[j] = v;
        }


        _timer.stop();
        return blockId;
    }

    private void loadBeforeUpdates(int interval, final ChiVertex<VertexDataType, EdgeDataType>[] vertices,  final MemoryShard<EdgeDataType> memShard,
                                   final int startVertex, final int endVertex) throws IOException {
        final Object terminationLock = new Object();
        final TimerContext _timer = loadTimer.time();
        // TODO: make easier to read
        synchronized (terminationLock) {
            // false
            final AtomicInteger countDown = new AtomicInteger(disableOutEdges ? 1 : nShards);

            if (!disableInEdges) {
                try {

                    logger.info("Memshard: " + startVertex + " -- " + endVertex);
                    memShard.loadVertices(startVertex, endVertex, vertices, disableOutEdges, parallelExecutor);
                    logger.info("Loading memory-shard finished." + Thread.currentThread().getName());

                    if (countDown.decrementAndGet() == 0) {
                        synchronized (terminationLock) {
                            terminationLock.notifyAll();
                        }
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }  catch (Exception err) {
                    err.printStackTrace();
                }
            }

            /* Load in parallel 并行加载  true */
            if (!disableOutEdges) {
                for(int p=0; p < nShards; p++) {
                    if (p != interval || disableInEdges) {
                        final int _p = p;
                        final SlidingShard<EdgeDataType> shard = slidingShards.get(p);
                        loadingExecutor.submit(new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    shard.readNextVertices(vertices, startVertex, false);
                                    if (countDown.decrementAndGet() == 0) {
                                        synchronized (terminationLock) {
                                            terminationLock.notifyAll();
                                        }
                                    }

                                } catch (IOException ioe) {
                                    ioe.printStackTrace();
                                    throw new RuntimeException(ioe);
                                }  catch (Exception err) {
                                    err.printStackTrace();
                                }
                            }
                        });
                    }
                }
            }

            // barrier
            try {
                while(countDown.get() > 0) {
                    terminationLock.wait(5000);
                    if (countDown.get() > 0) {
                        logger.info("Still waiting for loading, counter is: " + countDown.get());
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        _timer.stop();
    }

    /**返回当前的GraphChiContext对象
     * @return the current GraphChiContext object
     */
    public GraphChiContext getContext() {
        return chiContext;
    }

    // 边数
    public long numEdges() {
        long numEdges = 0;
        for(SlidingShard shard : slidingShards) {
            numEdges += shard.getNumEdges();
        }
        return numEdges;

    }

    class IntervalData {
        private VertexInterval subInterval;
        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private int vertexBlockId;
        private MemoryShard<EdgeDataType> memShard;
        private int intervalNum;

        IntervalData(VertexInterval subInterval, ChiVertex<VertexDataType, EdgeDataType>[] vertices, int vertexBlockId,
                     MemoryShard<EdgeDataType> memShard, int intervalNum) {
            this.subInterval = subInterval;
            this.vertices = vertices;
            this.vertexBlockId = vertexBlockId;
            this.intervalNum = intervalNum;
            this.memShard = memShard;
        }

        public VertexInterval getSubInterval() {
            return subInterval;
        }

        public ChiVertex<VertexDataType, EdgeDataType>[] getVertices() {
            return vertices;
        }

        public int getVertexBlockId() {
            return vertexBlockId;
        }

        public MemoryShard<EdgeDataType> getMemShard() {
            return memShard;
        }

        public int getIntervalNum() {
            return intervalNum;
        }
    }

    class AutoLoaderTask implements Callable<IntervalData> {

        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private VertexInterval interval;
        private MemoryShard<EdgeDataType> memShard;
        private int intervalNum;

        AutoLoaderTask(VertexInterval interval, int intervalNum, MemoryShard<EdgeDataType> memShard) {
            this.interval = interval;
            this.memShard = memShard;
            this.intervalNum = intervalNum;
            if (!onlyAdjacency) {
                throw new RuntimeException("Can use auto-loading only with only-adjacency mode!");
            }
        }

        @Override
        public IntervalData call() {
            try {
                int lastVertex  = determineNextWindow(interval.getFirstVertex(), interval.getLastVertex());
                int nVertices = lastVertex - interval.getFirstVertex() + 1;
                this.vertices = (ChiVertex<VertexDataType, EdgeDataType>[]) new ChiVertex[nVertices];
                int vertexBlockid = initVertices(nVertices, interval.getFirstVertex(), vertices);

                loadBeforeUpdates(intervalNum, vertices, memShard, interval.getFirstVertex(), lastVertex);
                return new IntervalData(new VertexInterval(interval.getFirstVertex(), lastVertex), vertices, vertexBlockid, memShard, intervalNum);

            } catch (NoEdgesInIntervalException nie) {
                return new IntervalData(new VertexInterval(interval.getFirstVertex(), interval.getLastVertex()), vertices, -1, memShard, intervalNum);

            } catch (Exception err) {
                err.printStackTrace();
                return null;
            }
        }


    }

    // 确定下一个窗口
    private int determineNextWindow(int subIntervalStart, int maxVertex) throws IOException, NoEdgesInIntervalException {
        final TimerContext _timer = determineNextWindowTimer.time();
        long totalDegree = 0;
        try {
            degreeHandler.load(subIntervalStart, maxVertex);

            if (useStaticWindowSize) {
                return maxVertex;
            }

            long memReq = 0;
            int maxInterval = maxVertex - subIntervalStart;
            int vertexDataSizeOf = (vertexDataConverter != null ? vertexDataConverter.sizeOf() : 0);
            int edataSizeOf = (onlyAdjacency ? 0 : edataConverter.sizeOf());

            logger.info("Memory budget: " + memBudget);
            //for(int i=0; i< maxInterval; i++)
            for(int i=0; i<= maxInterval; i++) {
                // false
                if (enableScheduler) {
                    if (!scheduler.isScheduled(i + subIntervalStart)) {
                        continue;
                    }
                }

                VertexDegree deg = degreeHandler.getDegree(i + subIntervalStart);
                int inc = deg.inDegree;
                int outc = deg.outDegree;

                // 当前顶点与其它顶点无关
                if (inc + outc == 0 && skipZeroDegreeVertices) {
                    continue;
                }

                // 当前顶点的总度数
                totalDegree += inc + outc;

                // Following calculation contains some perhaps reasonable estimates of the
                // overhead of Java objects.
                // 下面的计算包含了对Java对象开销的一些也许合理的估计。
                // 访问当前顶点也需要访问其邻居顶点

                memReq += vertexDataSizeOf + 256 + (edataSizeOf + 4 + 4 + 4) * (inc + outc);
                // 超出预算
                if (memReq > memBudget) {
                    if (totalDegree == 0 && vertexDataConverter == null) {
                        throw new NoEdgesInIntervalException();
                    }
                    return subIntervalStart + i - 1; // Previous vertex was enough 前一个顶点已经足够
                }
            }
            if (totalDegree == 0 && vertexDataConverter == null) {
                throw new NoEdgesInIntervalException();
            }
            return maxVertex;
        } finally {
            _timer.stop();
        }
    }

    // 是否启用调度程序
    public boolean isEnableScheduler() {
        return enableScheduler;
    }

    /**
     * Enabled the selective scheduling. By default, scheduling is not enabled.
     * @param enableScheduler
     */
    public void setEnableScheduler(boolean enableScheduler) {
        this.enableScheduler = enableScheduler;
    }

    /**
     * Sets the bytes->vertex value converter object.
     * @param vertexDataConverter
     */
    public void setVertexDataConverter(BytesToValueConverter<VertexDataType> vertexDataConverter) {
        this.vertexDataConverter = vertexDataConverter;
        this.hasSetVertexDataConverter = true;
    }

    /**
     * Sets the bytes->edge value converter object. If the object is null,
     * then no edge-values are read (only adjacency information).
     * @param edataConverter
     */
    public void setEdataConverter(BytesToValueConverter<EdgeDataType> edataConverter) {
        this.edataConverter = edataConverter;
        this.hasSetEdgeDataConverter = true;
    }

    // 是否启用确定性执行
    public boolean isEnableDeterministicExecution() {
        return enableDeterministicExecution;
    }

    /**
     * Enabled or disables the deterministic parallelism. It is enabled by default.
     * See http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi section "Parallel Updates"
     * @param enableDeterministicExecution
     */
    public void setEnableDeterministicExecution(boolean enableDeterministicExecution) {
        this.enableDeterministicExecution = enableDeterministicExecution;
    }

    //  是否禁止使用出边
    public boolean isDisableOutEdges() {
        return disableOutEdges;
    }

    /**
     * Disable loading of out-edges
     * 禁用出边的加载
     * @param disableOutEdges
     */
    public void setDisableOutEdges(boolean disableOutEdges) {
        this.disableOutEdges = disableOutEdges;
    }

    // 是否修改入边
    public boolean isModifiesInedges() {
        return modifiesInedges;
    }

    /**
     * Disable/enable writing of in-edges (enabled by default)
     * 禁用/启用写入边的功能（默认为启用）
     * @param modifiesInedges
     */
    public void setModifiesInedges(boolean modifiesInedges) {
        this.modifiesInedges = modifiesInedges;
    }

    // 是否修改了出边
    public boolean isModifiesOutedges() {
        return modifiesOutedges;
    }

    public void setModifiesOutedges(boolean modifiesOutedges) {
        this.modifiesOutedges = modifiesOutedges;
    }

    // 是否知识邻接
    public boolean isOnlyAdjacency() {
        return onlyAdjacency;
    }

    /**
     * Load only adjacency data.
     * 仅加载邻接数据
     * @param onlyAdjacency
     */
    public void setOnlyAdjacency(boolean onlyAdjacency) {
        this.onlyAdjacency = onlyAdjacency;
        this.hasSetEdgeDataConverter = true;
    }

    public void setDisableInedges(boolean b) {
        this.disableInEdges = b;
    }

    // 是否禁止使用入边
    public boolean isDisableInEdges() {
        return disableInEdges;
    }

    // 获取最大窗口
    public int getMaxWindow() {
        return maxWindow;
    }

    /**
     * Configures the maximum number of vertices loaded at any time.
     * Default is 20 million. Generally you should not needed to modify this.
     * 配置任何时候加载的最大顶点数量。
     * 默认是2000万。一般来说，你应该不需要修改这个参数。
     * @param maxWindow
     */
    public void setMaxWindow(int maxWindow) {
        this.maxWindow = maxWindow;
    }

    // 是否使用静态窗口大小
    public boolean isUseStaticWindowSize() {
        return useStaticWindowSize;
    }

    /**
     * Enables use of static window size (without adjusting the number
     * of vertices loaded at any time based on the amount of available memory).
     * Only for advanced users!
     * @param useStaticWindowSize
     */
    public void setUseStaticWindowSize(boolean useStaticWindowSize) {
        this.useStaticWindowSize = useStaticWindowSize;
    }

    // 是否自动导入下一个
    public boolean isAutoLoadNext() {
        return autoLoadNext;
    }


    /**
     * Experimental feature that enables GraphChi to load data ahead.
     * This works only with onlyAdjacency-setting. DO NOT USE - NOT TESTED.
     * @param autoLoadNext
     */
    public void setAutoLoadNext(boolean autoLoadNext) {
        this.autoLoadNext = autoLoadNext;
    }

    // 模拟调度程序
    private class MockScheduler implements Scheduler {

        @Override
        public void addTask(int vertexId) {

        }

        @Override
        public void removeTasks(int from, int to) {
        }

        @Override
        public void addAllTasks() {

        }

        @Override
        public boolean hasTasks() {
            return true;
        }

        @Override
        public boolean isScheduled(int i) {
            return true;
        }

        @Override
        public void removeAllTasks() {

        }

        @Override
        public void scheduleOutNeighbors(ChiVertex vertex) {
        }

        @Override
        public void scheduleInNeighbors(ChiVertex vertex) {
        }
    }

    /**
     * GraphChi uses internal vertex ids. To translate from the internal ids
     * to the ids used in the original graph, obtain VertexIdTranslate object
     * by using this method and call translater.backward(internalId)
     * GraphChi使用内部顶点ID。要从内部id翻译成原图中使用的id，
     * 请使用此方法获得VertexIdTranslate对象，
     * 并调用translater.backward(internalId)
     * @return
     */
    public VertexIdTranslate getVertexIdTranslate() {
        return vertexIdTranslate;
    }

    public void setVertexIdTranslate(VertexIdTranslate vertexIdTranslate) {
        this.vertexIdTranslate = vertexIdTranslate;
    }

    private class GraphChiContextInternal extends GraphChiContext{
        @Override
        protected void setVertexIdTranslate(VertexIdTranslate vertexIdTranslate) {
            super.setVertexIdTranslate(vertexIdTranslate);
        }

        @Override
        public void setThreadLocal(Object threadLocal) {
            super.setThreadLocal(threadLocal);
        }

        @Override
        protected void setNumVertices(long numVertices) {
            super.setNumVertices(numVertices);
        }

        @Override
        protected void setNumEdges(long numEdges) {
            super.setNumEdges(numEdges);
        }

        @Override
        protected void setScheduler(Scheduler scheduler) {
            super.setScheduler(scheduler);
        }

        @Override
        protected void setNumIterations(int numIterations) {
            super.setNumIterations(numIterations);
        }

        @Override
        protected void setIteration(int iteration) {
            super.setIteration(iteration);
        }

        @Override
        protected void setCurInterval(VertexInterval curInterval) {
            super.setCurInterval(curInterval);
        }
    }
}

class NoEdgesInIntervalException extends Exception {
}