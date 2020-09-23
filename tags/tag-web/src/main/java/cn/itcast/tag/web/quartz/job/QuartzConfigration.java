package cn.itcast.tag.web.quartz.job;

import java.io.IOException;
import java.util.Properties;

import cn.itcast.tag.web.job.JobFactory;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 * 
 * @author liuchengli
 *
 */
@Configuration
public class QuartzConfigration {
	 @Autowired
	 private JobFactory jobFactory;
	 
	    @Bean
	    public SchedulerFactoryBean schedulerFactoryBean() {
	      SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
	      try {
			schedulerFactoryBean.setQuartzProperties(quartzProperties());
			schedulerFactoryBean.setJobFactory(jobFactory);
		} catch (IOException e) {
			e.printStackTrace();
		}
	      return schedulerFactoryBean;
	    }
	    
	  //指定quartz.properties
	    @Bean
	    public Properties quartzProperties() throws IOException {
	        PropertiesFactoryBean propertiesFactoryBean = new PropertiesFactoryBean();
	        propertiesFactoryBean.setLocation(new ClassPathResource("/quartz.properties"));
	        propertiesFactoryBean.afterPropertiesSet();
	        return propertiesFactoryBean.getObject();
	    }
	 
	//创建schedule  
	    @Bean(name = "scheduler")
	    public Scheduler scheduler() {
	      return schedulerFactoryBean().getScheduler();
	    }
	    
}
