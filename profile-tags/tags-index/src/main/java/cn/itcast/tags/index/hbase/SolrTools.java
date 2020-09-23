package cn.itcast.tags.index.hbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * @Author Harry
 * @Date 2020-09-01 13:10
 * @Description 编写工具类进行封装
 */
public class SolrTools {

    // 创建Config对象，加载配置文件
    private static Config config = ConfigFactory.load("solr.properties");
    // Solr url 的地址
    private static String solrUrl = config.getString("solr.addr");

    // Solr 索引库中字段名称
    public final static String[] fields =
            config.getString("solr.index.tags.fields").split(",") ;
    // HBase表画像标签数据列簇和列名称
    public final static String family =
            config.getString("solr.hbase.profile.family");
    public final static String[] columns =
            config.getString("solr.hbase.profile.fields").split(",");


    /**
     * 依据索引Doc中id字段删除索引
     * @param userId 标识符ID
     */
    public static void deleteDoc(String userId) {
        // 1. 连接solr 根据Id进行删除
        HttpSolrServer solrServer = null;
        try {
            //2 构建SolrServer对象
            solrServer = new HttpSolrServer(solrUrl);
            //3 根据id进行删除
            solrServer.deleteById(userId);
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            //4 关闭服务
            if (null != solrServer) solrServer.shutdown();
        }

    }
    /**
     * 向Solr索引库添加索引
     * @param doc SolrInputDocument对象
     */
    public static void addDoc(SolrInputDocument doc) {
        HttpSolrServer solrServer = null;
        try {
            //3.1 构建SolrServer对象
            solrServer = new HttpSolrServer(solrUrl);
            //3.2 添加数据
            solrServer.add(doc);
            //3.3 提交任务
            solrServer.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            //3.4 关闭服务
            if (null != solrServer) solrServer.shutdown();
        }

    }
}
