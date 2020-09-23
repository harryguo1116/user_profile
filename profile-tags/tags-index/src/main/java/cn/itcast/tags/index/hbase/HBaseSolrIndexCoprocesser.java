package cn.itcast.tags.index.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

/**
 * @Author Harry
 * @Date 2020-09-01 12:16
 * @Description 当新增、删除用户持有的标签时，会同步索引到solr中
 */
public class HBaseSolrIndexCoprocesser extends BaseRegionObserver {

    // 定义HBase表需要使用的变量
    private final String solrUrl = "http://bigdata-cdh01.itcast.cn:8983/solr/tags";
    private final byte[] family = Bytes.toBytes("user");
    private final String[] columns = new String[]{"userId", "tagIds"};

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {


        //1. 构建Solr Document对象
        SolrInputDocument document = new SolrInputDocument();
        //2. 依据列簇和列名获取值
        for (String column : columns) {
            List<Cell> cells = put.get(family, column.getBytes());
            // 对cells进行判断  非空再进行获取
            if (null != cells && cells.size() > 0) {
                // HBase表中的数据多个版本  获取第一个版本 下标为0
                Cell cell = cells.get(0);
                // 从cell中获取值
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                // 设置Solr Doc 中的字段 先对字段进行判断
                if ("userId".equals(column)) {
                    document.addField("user_id", value);
                } else if ("tagIds".equals(column)) {
                    document.addField("tag_ids", value);
                }
            }
        }

        //3. 提交数据
        HttpSolrServer solrServer = null;
        try {
            //3.1 构建SolrServer对象
            solrServer = new HttpSolrServer(solrUrl);
            //3.2 添加数据
            solrServer.add(document);
            //3.3 提交任务
            solrServer.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            //3.4 关闭服务
            if (null != solrServer) solrServer.shutdown();
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete, WALEdit edit, Durability durability) throws IOException {

        // 1. 获取RowKey就是要删除的ID
        String userId = Bytes.toString(delete.getRow());
        // 2. 连接solr 根据Id进行删除
        HttpSolrServer solrServer = null;
        try {
            //3.1 构建SolrServer对象
            solrServer = new HttpSolrServer(solrUrl);
            //3.2 根据id进行删除
            solrServer.deleteById(userId);
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            //3.3 关闭服务
            if (null != solrServer) solrServer.shutdown();
        }
    }
}
