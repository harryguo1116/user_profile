package cn.itcast.tags.index.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author Harry
 * @Date 2020-08-31 20:54
 * @Description 与Solr服务器交互，向Solr中插入Insert、删除Delete索引
 */
public class SolrIndexTest {
    String solrUrl = "http://bigdata-cdh01.itcast.cn:8983/solr/orders";

    //1. 入门案例  向Solr中插入索引 单条插入 当ID相同时为修改
    @Test
    public void createIndexToSolr() throws IOException, SolrServerException {

        // a. 创建SolrServer对象
        HttpSolrServer solrServer = new HttpSolrServer(solrUrl);

        // b. 添加document文档对象
        SolrInputDocument doc = new SolrInputDocument();

        /*doc.addField("order_id", "10001");
        doc.addField("order_amt", 98.34);
        doc.addField("address", "江苏省南京市");*/

        /*doc.addField("order_id", "10002");
        doc.addField("order_amt", 88.88);
        doc.addField("address", "江苏省苏州市");*/
        doc.addField("order_id", "10003");
        doc.addField("order_amt", 66.66);
        doc.addField("address", "上海市浦东新区");
        // c. 进行索引添加
        solrServer.add(doc);
        // d. 提交索引
        solrServer.commit();
        solrServer.shutdown();

    }

    // 2. 查询检索
    @Test
    public void queryIndexToSolr() throws SolrServerException {

        //a. 创建SolrServer对象
        HttpSolrServer solrServer = new HttpSolrServer(solrUrl);
        //b. 获得query对象 查询索引
        SolrQuery query = new SolrQuery("*:*");
//        SolrQuery query = new SolrQuery("address:南京 OR address:苏州");
        QueryResponse response = solrServer.query(query);
        //c. 获得response对象中的信息
        SolrDocumentList results = response.getResults();
        for (SolrDocument result : results) {
            Object order_id = result.get("order_id");
            Object order_amt = result.get("order_amt");
            Object address = result.get("address");
            System.out.println("orderId -> " + order_id + "\t" + " orderAmt -> " + order_amt + "\t" + " address -> " + address);
        }
        //d. 关闭服务
        solrServer.shutdown();
    }

    // 3. 删除索引，依据ID删除或者条件删除
    @Test
    public void deleteIndexToSolr() throws IOException, SolrServerException {
        //a. 创建SolrServer对象
        HttpSolrServer solrServer = new HttpSolrServer(solrUrl);
        //b. 获取要删除的id 根据id进行删除
//        solrServer.deleteById("10001");
        // 根据条件进行删除
        solrServer.deleteByQuery("*:*");
        //c. 执行提交 关闭服务
        solrServer.commit();
        solrServer.shutdown();
    }


}
