package cn.itcast.tag.web.engine.service;

import java.util.List;

import cn.itcast.tag.web.user.bean.UserBean;
import cn.itcast.tag.web.engine.bean.EngineBean;

/**
 * MetaDataService
 * @author liuchengli
 *
 */
public interface EngineService {
    /**
     * 启动oozie 定时任务
     * @param bean
     * @return
     */
	public boolean startEngine(EngineBean bean, UserBean userBean);
    
	/**
     * 停用当前定时任务
     * @param bean
     * @return
     */
	public boolean stopEngine(EngineBean bean, UserBean userBean);
	
	/**
	 * 暂停当时任务
	 * @param bean
	 * @return
	 */
	public boolean suspendEngine(EngineBean bean);
	/**
	 * 根据条件查询
	 * @param bean
	 * @return
	 */
	public List<EngineBean> queryMonitorInfoByAll(EngineBean bean);
	
    /**
     * 启动quartz 定时任务
     * @param bean
     * @return
     */
	public boolean startEngineByQuartz(EngineBean bean, UserBean userBean);
   
	/**
     * 停用当前quartz定时任务
     * @param bean
     * @return
     */
	public boolean stopEngineByQuartz(EngineBean bean, UserBean userBean);
}
