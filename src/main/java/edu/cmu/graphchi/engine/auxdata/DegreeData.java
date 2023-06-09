package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import ucar.unidata.io.RandomAccessFile;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
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
 * GraphChi keeps track of the degree of each vertex (count of in- and out-edges). This class
 * allows accessing the vertex degrees efficiently by loading a window
 * of the edges a time. This supports both sparse and dense representation
 * of the degrees. This class should not be needed by application writers, and
 * is only used internally by GraphChi.
 *
 * GraphChi记录了每个顶点的度数（进出的边数）。
 * 这个类允许通过每次加载一个窗口来有效地访问顶点度数
 * 每次加载一个窗口的边。这支持稀疏和密集的表示法度的表示。
 * 这个类不应该被应用程序的作者所需要，而只在GraphChi内部使用。
 * 只在GraphChi内部使用。
 * @author Aapo Kyrola
 */
public class DegreeData {

    private RandomAccessFile degreeFile;

    private byte[] degreeData;
    private int vertexSt, vertexEn;

    private boolean sparse = false;
    private int lastQuery = 0, lastId = -1;

    public DegreeData(String baseFilename) throws IOException {
        File sparseFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, true));
        File denseFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, false));

        // false
        if (sparseFile.exists()) {
            sparse = true;
            degreeFile = new RandomAccessFile(sparseFile.getAbsolutePath(), "r");
        } else {
            sparse = false;
            degreeFile = new RandomAccessFile(denseFile.getAbsolutePath(), "r");
        }
        vertexEn = vertexSt = 0;
    }

    /**
     * Load degrees for an interval of vertices
     * 为一个顶点的区间加载度数
     * @param _vertexSt first vertex
     * @param _vertexEn last vertex (inclusive)
     * @throws IOException
     */
    public void load(int _vertexSt, int _vertexEn) throws IOException {

        int prevVertexEn = vertexEn;
        int prevVertexSt = vertexSt;

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        long dataSize =  (long)  (vertexEn - vertexSt + 1) * 4 * 2;

        byte[] prevData = degreeData;
        degreeData = new byte[(int)dataSize];
        int len = 0;

        // Little bit cmoplicated book keeping to avoid redundant reads
        if (prevVertexEn > _vertexSt && prevVertexSt <= _vertexSt) {
            // Copy previous
            len = (int) ((prevVertexEn - vertexSt + 1) * 8);
            System.arraycopy(prevData, prevData.length - len, degreeData, 0, len);
        }


        if (!sparse) {
            int adjLen = (int) (dataSize - len);

            if (adjLen == 0) {
                return;
            }
            long dataStart =  (long)  vertexSt * 4L * 2L + len;

            try {
                degreeFile.seek(dataStart);
                degreeFile.readFully(degreeData, (int)(dataSize - adjLen), adjLen);
            } catch (EOFException eof) {
            	ChiLogger.getLogger("engine").info("Error: Tried to read past file: " + dataStart + " --- " + (dataStart + dataSize));
                // But continue
            }
        } else {
            if (lastQuery > _vertexSt) {
                lastId = -1;
                degreeFile.seek(0);
            }

            try {
                while(true) {
                    int vertexId = (lastId < 0 ? degreeFile.readInt() : lastId);
                    if (vertexId >= _vertexSt && vertexId <= _vertexEn) {
                        degreeFile.readFully(degreeData, (vertexId - vertexSt) * 8, 8);
                        VertexDegree deg = getDegree(vertexId);
                        lastId = -1;
                    } else if (vertexId > vertexEn){
                        lastId = vertexId; // Remember last one read
                        break;
                    } else {
                        degreeFile.skipBytes(8);
                    }
                }
            } catch (EOFException eof) {
                degreeFile.seek(0);
            }
            lastQuery = _vertexEn;
        }
    }

    /**
     * Returns degree of a vertex. The vertex must be in the previous
     * interval loaded using load().
     * 返回一个顶点的等级。该顶点必须是在使用load()加载的前一个区间内。
     * @param vertexId id of the vertex
     * @return  VertexDegree object
     */
    public VertexDegree getDegree(int vertexId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);

        byte[] tmp = new byte[4];
        int idx = vertexId - vertexSt;
        // degreeData中 对于每个顶点放的是入度和出度相邻，在 FastSharder 中存入，且都为 int 类型
        // 通过 Integer.reverseBytes() 方法把int类型的整数的二进制位按照字节（1个字节等于8位）进行反转
        // int = 4 byte
        // RandomAccessFile 中以 byte为单位通过 pos 来访问和修改
        // 所以
        System.arraycopy(degreeData, idx * 8, tmp, 0, 4);

        int indeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        System.arraycopy(degreeData, idx * 8 + 4, tmp, 0, 4);
        int outdeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        return new VertexDegree(indeg, outdeg);
    }
}
