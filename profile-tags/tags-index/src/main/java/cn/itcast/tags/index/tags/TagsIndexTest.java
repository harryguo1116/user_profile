package cn.itcast.tags.index.tags;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author Harry
 * @Date 2020-08-31 21:55
 * @Description 用户标签索引数据tags
 */
public class TagsIndexTest {
    String solrUrl = "http://bigdata-cdh01.itcast.cn:8983/solr/tags";
    HttpSolrServer solrServer = null;

    /*
    "user_id", "tag_ids"
     */
    @Before
    public void init() {
        // 构建SolrServer对象
        solrServer = new HttpSolrServer(solrUrl);
    }

    @Test
    public void createIndexToSolr() throws IOException, SolrServerException {
        // 构建插入字段的对象
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("user_id", "100");
        doc.addField("tag_ids", "321,334,350");
//        doc.addField("user_id","101");
//        doc.addField("tag_ids","323,354,370");
//        doc.addField("user_id","102");
//        doc.addField("tag_ids","321,354,350");
        // 提交插入字段的任务
        solrServer.add(doc);
        solrServer.commit();
    }

    // 2. 删除索引，依据ID删除或者条件删除
    @Test
    public void deleteIndexToSolr() throws IOException, SolrServerException {
        // 根据id进行删除
//        solrServer.deleteById("100");
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }

    // 3. 布尔条件查询
    @Test
    public void booleanQueryToSolr() throws SolrServerException {
        //执行查询
        /**
         * 1.布尔查询:
         * AND OR NOT:
         * AND : MUST
         * OR: SHOULD
         * NOT : MUST_NOT
         */
        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryResponse response = solrServer.query(solrQuery);
        SolrDocumentList documents = response.getResults();
        for (SolrDocument document : documents) {
            Object user_id = document.get("user_id");
            Object tag_ids = document.get("tag_ids");
            System.out.println("userId -> " + user_id + "\t" + "tag_ids -> " + tag_ids);
        }
    }

    @After
    public void close() {
        // 关闭资源
        solrServer.shutdown();
    }
}


