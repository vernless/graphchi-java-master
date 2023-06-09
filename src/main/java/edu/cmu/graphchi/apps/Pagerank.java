package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.IdFloat;
import edu.cmu.graphchi.util.Toplist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Example application: PageRank (http://en.wikipedia.org/wiki/Pagerank)
 * Iteratively computes a pagerank for each vertex by averaging the pageranks
 * of in-neighbors pageranks.
 * @author akyrola
 */
public class Pagerank implements GraphChiProgram<Float, Float> {

    private static Logger logger = ChiLogger.getLogger("pagerank");

    @Override
    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context)  {
        if (context.getIteration() == 0) {
            /* Initialize on first iteration
            * 在第一次迭代时进行初始化
            * */
            vertex.setValue(1.0f);
        } else {
            /* On other iterations, set my value to be the weighted
               average of my in-coming neighbors pageranks.
               在其他迭代中，将我的值设置为我的传入邻居页面排名的加权平均值。
             */
            //System.out.println("update");
            float sum = 0.f;
            for(int i=0; i<vertex.numInEdges(); i++) {
                sum += vertex.inEdge(i).getValue();
            }
            vertex.setValue(0.15f + 0.85f * sum);
        }

        /* Write my value (divided by my out-degree) to my out-edges so neighbors can read it.
        * 将我的值（除以我的出度数）写到我的出边，以便邻居可以读取它
        *  */
        float outValue = vertex.getValue() / vertex.numOutEdges();
        for(int i=0; i<vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(outValue);
        }

    }


    /**
     * Callbacks (not needed for Pagerank)
     */
    @Override
    public void beginIteration(GraphChiContext ctx) {}
    @Override
    public void endIteration(GraphChiContext ctx) {}
    @Override
    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}
    @Override
    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}
    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {}
    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {}

    /**
     * Initialize the sharder-program.
     * @param graphName
     * @param numShards
     * @return
     * @throws IOException
     */
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Float, Float>(graphName, numShards, new VertexProcessor<Float>() {
            @Override
            public Float receiveVertexValue(int vertexId, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new EdgeProcessor<Float>() {
            @Override
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new FloatConverter(), new FloatConverter());
    }

    /**
     * Usage: java edu.cmu.graphchi.demo.PageRank graph-name num-shards filetype(edgelist|adjlist)
     * For specifying the number of shards, 20-50 million edges/shard is often a good configuration.
     */
    public static void main(String[] args) throws  Exception {
        //String baseFilename = args[0];
        // baseFilename = "F:\\paper\\dataset\\CA-GrQc.txt";
        String baseFilename = "F:\\paper\\dataset\\testCA\\CA_test.txt";
        //int nShards = Integer.parseInt(args[1]);
        int nShards = 1;
        //String fileType = (args.length >= 3 ? args[2] : null);
        String fileType = "edgelist";

        CompressedIO.disableCompression();

        /* Create shards */
        FastSharder sharder = createSharder(baseFilename, nShards);
        if (baseFilename.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in, fileType);
        } else {
            if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
            } else {
                logger.info("Found shards -- no need to preprocess");
            }
        }

        /* Run GraphChi */

        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(baseFilename, nShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatConverter());
        // 不能修改入边
        engine.setModifiesInedges(false); // Important optimization

        Long start = System.currentTimeMillis();
        engine.run(new Pagerank(), 100);
        Long end = System.currentTimeMillis();
        logger.info("处理时间：" + (end - start) * 0.001 + " secs.");

        /* Output results */
        int i = 0;
        VertexIdTranslate trans = engine.getVertexIdTranslate();
        TreeSet<IdFloat> top20 = Toplist.topListFloat(baseFilename, engine.numVertices(), 20);
        for(IdFloat vertexRank : top20) {
            System.out.println(++i + ": " + trans.backward(vertexRank.getVertexId()) + " = " + vertexRank.getValue());
        }

//        ArrayList<VertexInterval> vertexIntervals = ChiFilenames.loadIntervals("F:\\paper\\dataset\\testCA\\CA_test.txt", 1);
//        for(VertexInterval v : vertexIntervals){
//            System.out.println(v.toString());
//        }
    }
}
