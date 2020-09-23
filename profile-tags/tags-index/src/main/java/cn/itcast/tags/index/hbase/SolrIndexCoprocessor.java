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
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

/**
 * @Author Harry
 * @Date 2020-09-01 13:40
 * @Description 当新增、删除用户持有的标签时，会同步索引到solr中
 */
public class SolrIndexCoprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {

        //1. 构建Solr Document对象
        SolrInputDocument document = new SolrInputDocument();

        String[] columns = SolrTools.columns;
        String[] fields = SolrTools.fields;

        //2. 依据列簇和列名获取值
        for (String column : columns) {
            List<Cell> cells = put.get(SolrTools.family.getBytes(), column.getBytes());
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
        SolrTools.addDoc(document);

    }


    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete, WALEdit edit, Durability durability) throws IOException {

        // 1. 获取RowKey就是要删除的ID
        String userId = Bytes.toString(delete.getRow());
        // 2. 依据ID删除索引
        SolrTools.deleteDoc(userId);


    }
}
