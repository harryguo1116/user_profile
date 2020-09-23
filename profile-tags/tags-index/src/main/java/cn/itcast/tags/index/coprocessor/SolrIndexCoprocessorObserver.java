package cn.itcast.tags.index.coprocessor;

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
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @Author Harry
 * @Date 2020-09-01 14:30
 * @Description 监听HBase，一有数据postPut就向Solr发送
 * - HBase有两种coprocessor，一种是Observer（观察者），类似于关系数据库的trigger（触发器）
 * - 另外一种就是EndPoint，类似于关系数据库的存储过程
 * 使用SolrWrite进行写数据
 */
public class SolrIndexCoprocessorObserver extends BaseRegionObserver {
    private static final Logger logger = LoggerFactory.getLogger(SolrIndexCoprocessorObserver.class);

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {

        String[] columns = SolrWriter.columns;
        String[] fields = SolrWriter.fields;
        try {
            //1. 创建SolrDocument 对象
            SolrInputDocument document = new SolrInputDocument();
            //2. 从put中获取数据
            for (int i = 0; i < columns.length; i++) {
                List<Cell> cells = put.get(SolrWriter.family.getBytes(), columns[i].getBytes());
                //对cells进行判断 非空再调用
                if (null != cells && cells.size() > 0) {
                    // 获取第一个版本
                    Cell cell = cells.get(0);
                    // 从cell中获取值
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    // 添加到document中
                    document.addField(fields[i], value);
                }
            }
            logger.warn(document.toString());
            //3. 批量添加数据
            SolrWriter.addDocToCache(document);
            logger.info("postPut 向Solr缓存中插入数据成功");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete, WALEdit edit, Durability durability) throws IOException {

        //1. 根据RowKey获取需要删除的ID
        String userId = Bytes.toString(delete.getRow());
        //2. 根据id进行删除数据
        SolrWriter solrWriter = new SolrWriter();
        try {
            logger.info("PostDelete 删除solr中的数据");
            solrWriter.deleteDoc(userId);
        } catch (SolrServerException exception) {
            exception.printStackTrace();
        }
    }
}
