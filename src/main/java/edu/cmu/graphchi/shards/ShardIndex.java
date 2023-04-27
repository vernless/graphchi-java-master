package edu.cmu.graphchi.shards;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Encapsulates a sparse index to shard's edges.
 * Can be used for fast queries or for parallelizing access (memoryshard).
 * 将稀疏索引封装到分片的边缘。可用于快速查询或并行化访问（内存分片）。
 */
public class ShardIndex {
    File indexFile;
    private int[] vertices;
    private int[] edgePointer;
    private int[] fileOffset;

    public ShardIndex(File adjFile) throws IOException {
        this.indexFile = new File(adjFile.getAbsolutePath() + ".index");
        load();
    }

    private void load() throws IOException {
        int n = (int) (indexFile.length() / 12) + 1;

        vertices = new int[n];
        edgePointer = new int[n];
        fileOffset = new int[n];

        vertices[0] = 0;
        edgePointer[0] = 0;
        fileOffset[0] = 0;

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
        int i = 1;

        while (i < n) {
            int v1 = dis.readInt();
            vertices[i] = v1;
            //System.out.println(v1 + "--");
            int v2 = dis.readInt();
            fileOffset[i] = v2;
            //System.out.println(v2 + "++");
            int v3 = dis.readInt();
            edgePointer[i] = v3;
            //System.out.println(v3 + "**");
            //System.out.println();
            i++;
        }
    }

    /**
     * Returns a sparsified index, which starts with a zero-pointer (and is thus always non-empty)
     * 返回一个稀疏的索引，它以一个零指针开始（因此总是非空的）。
     */
    public ArrayList<IndexEntry> sparserIndex(int edgeDistance) {
        ArrayList<IndexEntry> spIdx = new ArrayList<IndexEntry>();
        spIdx.add(new IndexEntry(0, 0, 0));
        int lastEdgePointer = 0;
        for(int j=0; j<vertices.length; j++) {
            if (edgePointer[j] - lastEdgePointer >= edgeDistance) {
                 spIdx.add(new IndexEntry(vertices[j], edgePointer[j], fileOffset[j]));
                 lastEdgePointer = edgePointer[j];
            }

        }
        return spIdx;
    }

    /**
     * Finds closest index entry for given vertex id, which is before the vertex.
     * @param vertexId
     * @return
     */
    public IndexEntry lookup(int vertexId) {
        int idx = Arrays.binarySearch(vertices, vertexId);
        if (idx >= 0) {
            return new IndexEntry(vertexId, edgePointer[idx], fileOffset[idx]);
        } else {
            idx = -(idx + 1) - 1;
            return new IndexEntry(vertices[idx], edgePointer[idx], fileOffset[idx]);
        }

    }



    public static class IndexEntry {

        public int vertex, edgePointer, fileOffset;

        IndexEntry(int vertex, int edgePointer, int fileOffset) {
            this.vertex = vertex;
            this.edgePointer = edgePointer;
            this.fileOffset = fileOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IndexEntry that = (IndexEntry) o;

            if (edgePointer != that.edgePointer) {
                return false;
            }
            if (fileOffset != that.fileOffset) {
                return false;
            }
            if (vertex != that.vertex) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = vertex;
            result = 31 * result + edgePointer;
            result = 31 * result + fileOffset;
            return result;
        }

        @Override
        public String toString() {
            return "vertex: " + vertex + ", offset=" + fileOffset;
        }
    }
}