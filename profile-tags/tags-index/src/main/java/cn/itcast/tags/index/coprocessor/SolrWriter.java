package cn.itcast.tags.index.coprocessor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * @Author Harry
 * @Date 2020-09-01 14:27
 * @Description  向Solr中写入数据（每隔一段时间）或删除数据
 */
public class SolrWriter {
    private static final Logger logger =
            LoggerFactory.getLogger(SolrWriter.class);
    // Solr Url地址
    private static String solrUrl ;
    // SolrServer 对象声明
    private static SolrServer solrServer = null;
    // Solr 索引库中字段名称
    static String[] fields ;
    // HBase表画像标签数据列簇和列名称
    static String family ;
    static String[] columns ;
    // 缓存大小，当达到该上限时提交
    private static int maxCacheCount ;
    // 缓存，使用Vector线程安全
    private static Vector<SolrInputDocument> cache = null;
    // 最大提交时间，单位是秒
    private static int maxCommitTime ;
    // 在添加缓存或进行提交时加锁
    private static Lock commitLock = new ReentrantLock();

    // TODO: 静态代码块，加载class字节码的时候执行
    static {
        // 创建Config对象，加载配置文件：solr.conf
        Config config = ConfigFactory.load("solr.conf");
        // 初始化各个值
        solrUrl = config.getString("solr.addr") ;
        fields = config.getString("solr.index.tags.fields").split(",") ;
        family = config.getString("solr.hbase.profile.family");
        columns = config.getString("solr.hbase.profile.fields").split(",");
        // 缓存配置
        maxCacheCount = config.getInt("solr.cache.max.count") ;
        maxCommitTime = config.getInt("solr.cache.commit.time") ;
        try {
            // 构建SolrServer对象
            solrServer = new HttpSolrServer(solrUrl) ;
            // 初始化缓冲
            cache = new Vector<SolrInputDocument>(maxCacheCount);

            // 启动定时任务，第一次延迟1s执行,之后每隔指定时间执行一次
            Timer timer = new Timer();
            timer.schedule(
                    new CommitTimer(),
                    1000, // 延迟1s执行
                    maxCommitTime * 1000 // 每隔指定时间间隔执行一次
            );
        }catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 添加记录到Cache，如果Cache达到maxCacheCount，则提交Commit
     */
    public static void addDocToCache(SolrInputDocument doc) {
        commitLock.lock(); // 加锁
        try {
            cache.add(doc);
            //cache满则提交
            if (cache.size() >= maxCacheCount) {
                logger.info("cache commit, count:" + cache.size());
                // 将缓冲池Cache中所有数据批量写入Solr中
                new SolrWriter().inputDoc(cache);
                // TODO: 清空缓存中数据
                cache.clear();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 批量提交
     */
    public void inputDoc(List<SolrInputDocument> docList) throws IOException, SolrServerException {
        if (docList != null && docList.size() > 0) {
            solrServer.add(docList);
            solrServer.commit();
        }
    }

    /**
     * 依据Solr索引中ID删除文档数据
     */
    public void deleteDoc(String rowkey) throws IOException, SolrServerException {
        solrServer.deleteById(rowkey);
        solrServer.commit();
    }

    /**
     * 提交定时器
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                //cache中有数据则提交
                if (cache.size() > 0) {
                    logger.info("timer commit, count:" + cache.size());
                    new SolrWriter().inputDoc(cache);
                    cache.clear(); // 清空缓存
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                commitLock.unlock();
            }
        }
    }
}
