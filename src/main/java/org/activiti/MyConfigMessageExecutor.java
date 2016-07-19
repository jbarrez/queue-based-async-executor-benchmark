package org.activiti;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.persistence.StrongUuidGenerator;
import org.activiti.spring.SpringProcessEngineConfiguration;
import org.activiti.spring.executor.jms.JobMessageListener;
import org.activiti.spring.executor.jms.MessageBasedJobManager;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ErrorHandler;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

@Configuration
public class MyConfigMessageExecutor {

  private static Logger logger = LoggerFactory.getLogger(MyConfigMessageExecutor.class);
  
  @Bean
  public DataSource dataSource() {
    String jdbcUrl = getStringProperty("jdbc-url");
    System.out.println("Jdbc url " + jdbcUrl);
    String jdbcUsername = getStringProperty("jdbc-username");
    System.out.println("Jdbc username " + jdbcUsername);
    String jdbcPassword = getStringProperty("jdbc-password");
    System.out.println("Jdbc password " + jdbcPassword);
    String jdbcDriver = getStringProperty("jdbc-driver");
    System.out.println("Jdbc driver " + jdbcDriver);

    if (jdbcUrl == null || jdbcUsername == null || jdbcPassword == null) {
      System.err.println("Invalid jdbc settings");
      throw new RuntimeException("Jdbc settings missing");
    }

    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(jdbcUrl);
    dataSource.setDriverClassName(jdbcDriver);
    dataSource.setUsername(jdbcUsername);
    dataSource.setPassword(jdbcPassword);

    String mindIdleString = getStringProperty("connections-min");
    Integer minIdle = null;
    if (mindIdleString  != null && !"".equals(mindIdleString)) {
      minIdle = Integer.valueOf(mindIdleString);
    }
    if (minIdle != null) {
      dataSource.setMinimumIdle(minIdle);
    }

    String maxPoolSizeString = getStringProperty("connections-max");
    Integer maxPoolSize = null;
    if (maxPoolSizeString  != null && !"".equals(maxPoolSizeString)) {
      maxPoolSize = Integer.valueOf(maxPoolSizeString);
    }
    if (maxPoolSize == null) {
      maxPoolSize = 100;
    }
    dataSource.setMaximumPoolSize(maxPoolSize);

    return dataSource;
  }
  
  @Bean(name = "transactionManager")
  public PlatformTransactionManager transactionManager() {
    DataSourceTransactionManager transactionManager = new DataSourceTransactionManager();
    transactionManager.setDataSource(dataSource());
    return transactionManager;
  }
  
  @Bean
  public SpringProcessEngineConfiguration processEngineConfiguration() {
    SpringProcessEngineConfiguration configuration = new SpringProcessEngineConfiguration();
    configuration.setIdGenerator(new StrongUuidGenerator());
    configuration.setDataSource(dataSource());
    configuration.setTransactionManager(transactionManager());
    configuration.setDatabaseSchemaUpdate(getStringProperty("schema-update"));
    configuration.setAsyncExecutorDefaultTimerJobAcquireWaitTime(3000);
    configuration.setAsyncExecutorMessageQueueMode(true);
    configuration.setAsyncExecutorActivate(true);

    MessageBasedJobManager jobManager = jobManager();
    jobManager.setProcessEngineConfiguration(configuration);
    configuration.setJobManager(jobManager);

    return configuration;
  }
  
  @Bean
  public ProcessEngine ProcessEngine() {
    return processEngineConfiguration().buildProcessEngine();
  }
  
  @Bean
  public MessageBasedJobManager jobManager() {
    MessageBasedJobManager jobManager = new MessageBasedJobManager(null);
    jobManager.setJmsTemplate(jmsTemplate());
    return jobManager;
  }

  @Bean
  public ConnectionFactory connectionFactory() {
    ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
    activeMQConnectionFactory.setBrokerURL(getStringProperty("broker-url"));
    activeMQConnectionFactory.setUseAsyncSend(true);
    return new CachingConnectionFactory(activeMQConnectionFactory);
  }
  
  @Bean
  public JmsTemplate jmsTemplate() {
      JmsTemplate jmsTemplate = new JmsTemplate();
      jmsTemplate.setDefaultDestination(new ActiveMQQueue(getStringProperty("queue")));
      jmsTemplate.setConnectionFactory(connectionFactory());
      return jmsTemplate;
  }
  
  @Bean
  public MessageListenerContainer messageListenerContainer() {
      DefaultMessageListenerContainer messageListenerContainer = new DefaultMessageListenerContainer();
      messageListenerContainer.setConnectionFactory(connectionFactory());
      messageListenerContainer.setDestinationName(getStringProperty("queue"));
      messageListenerContainer.setMessageListener(jobMessageListener());
      messageListenerContainer.setConcurrentConsumers(getintProperty("concurrent-consumers"));
      messageListenerContainer.setErrorHandler(new ErrorHandler() {
        public void handleError(Throwable throwable) {
          logger.error("Error while handling JMS message", throwable);
        }
      });
      messageListenerContainer.start();
      return messageListenerContainer;
  }
  
  @Bean 
  public JobMessageListener jobMessageListener() {
    JobMessageListener jobMessageListener = new JobMessageListener();
    jobMessageListener.setProcessEngineConfiguration(processEngineConfiguration());
    return jobMessageListener;
  }
  
  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }


  private static String getStringProperty(String s) {
    return Main.properties.getProperty(s);
  }

  private static boolean getBooleanProperty(String s) {
    String value = Main.properties.getProperty(s);
    if (value != null) {
      return Boolean.valueOf(value);
    }
    return false;
  }

  private static int getintProperty(String s) {
    String value = Main.properties.getProperty(s);
    if (value != null) {
      return Integer.valueOf(value);
    }
    return -1;
  }


}
