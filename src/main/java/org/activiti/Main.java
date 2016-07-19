package org.activiti;

import com.zaxxer.hikari.HikariDataSource;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.ProcessEngineConfiguration;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.cfg.StandaloneProcessEngineConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final String MODE_PRODUCER = "producer";
    private static final String MODE_EXECUTOR = "executor";
    private static final String MODE_MESSAGE_EXECUTOR = "message-executor";
    private static final String MODE_MESSAGE_PRODUCER = "message-producer";

    public static Properties properties;

    private static String mode;
    private static boolean displayStats;
    private static int numberOfProcessInstances = -1;
    private static int delayBetweenProcessInstanceStart = -1;

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        loadProperties();

        mode = getStringProperty("mode");
        displayStats = getBooleanProperty("display-stats");
        numberOfProcessInstances = getintProperty("nr-process-instances");
        delayBetweenProcessInstanceStart = getintProperty("delay-between-process-instance-start");

        System.out.println("Mode = " + mode);
        System.out.println(displayStats ? "Displaying stats" : "<Stat display disabled>");
        if (numberOfProcessInstances > 0) {
            System.out.println("Starting " + numberOfProcessInstances + " process instances, one instance every " + delayBetweenProcessInstanceStart + " ms");
        }

        boolean enableAsyncExecutor = getintProperty("async-executor-max-nr-threads") > 0;

        ProcessEngine processEngine = null;
        if (MODE_MESSAGE_PRODUCER.equals(mode)) {

            System.out.println("Message producer mode enabled");
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MyConfigMessageProducer.class);
            processEngine = context.getBean(ProcessEngine.class);

        } else if (MODE_MESSAGE_EXECUTOR.equals(mode)) {

            System.out.println("Message executor mode enabled");
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MyConfigMessageExecutor.class);
            processEngine = context.getBean(ProcessEngine.class);

        } else {

            ProcessEngineConfiguration processEngineConfiguration = new StandaloneProcessEngineConfiguration();
            processEngineConfiguration.setDataSource(createDateSource());
            processEngineConfiguration.setDatabaseSchemaUpdate(getStringProperty("schema-update"));

            ((ProcessEngineConfigurationImpl) processEngineConfiguration).setAsyncExecutorDefaultTimerJobAcquireWaitTime(1000);
            ((ProcessEngineConfigurationImpl) processEngineConfiguration).setAsyncExecutorDefaultTimerJobAcquireWaitTime(5000);

            if (enableAsyncExecutor) {
                System.out.println("Async executor is enabled");
                processEngineConfiguration.setAsyncExecutorActivate(true);

                int executorMaxPoolSize = getintProperty("async-executor-max-nr-threads");
                System.out.println("Max nr of threads for async executor thread pool : " + executorMaxPoolSize);
                ((ProcessEngineConfigurationImpl) processEngineConfiguration).setAsyncExecutorMaxPoolSize(executorMaxPoolSize);

                int executorQueueSize = getintProperty("async-executor-queue-size");
                System.out.println("Async executor queue size " + executorQueueSize);
                ((ProcessEngineConfigurationImpl) processEngineConfiguration).setAsyncExecutorThreadPoolQueueSize(executorQueueSize);
            }

            processEngine = processEngineConfiguration.buildProcessEngine();
        }

        System.out.println("Process engine ready.");
        boolean startProcessInstances = numberOfProcessInstances > 0;
        if (startProcessInstances) {
            System.out.println("About to start process instances");
            RepositoryService repositoryService = processEngine.getRepositoryService();
            repositoryService.createDeployment().addClasspathResource("asyncProcess.bpmn20.xml").deploy();
            startProcessInstances(processEngine);
        }

        PrintWriter printWriter = null;
        if (displayStats) {
            System.out.println("<Displaying stats enabled>");
            File outputFile = new File("output.txt");
            FileWriter fileWriter = new FileWriter(outputFile);
            printWriter = new PrintWriter(fileWriter);

            printWriter.println("timeStamp;nrOfProcessInstances;nrOfExecutions;nrOfTasks;nrOfAsyncJobs;nrOfTimerJobs;nrOfDeadLetterJobs;nrFinishedProcessInstances;nrFinishedTasks;nrOfFinishedHistoricActInstances");
        }

        boolean allDone = false;
        while (!allDone) {

            if (displayStats) {

                long nrOfProcessInstances = processEngine.getRuntimeService().createProcessInstanceQuery().count();
                long nrOfExecutions = processEngine.getRuntimeService().createExecutionQuery().count();
                long nrOfTasks = processEngine.getTaskService().createTaskQuery().count();
                long nrOfAsyncJobs = processEngine.getManagementService().createJobQuery().count();
                long nrOfTimerJobs = processEngine.getManagementService().createTimerJobQuery().count();
                long nrDeadLetterJobs = processEngine.getManagementService().createDeadLetterJobQuery().count();
                long nrCompletedHistoricActivities = processEngine.getHistoryService().createHistoricActivityInstanceQuery().finished().count();

                long nrFinishedProcessInstances = processEngine.getHistoryService().createHistoricProcessInstanceQuery().finished().count();
                long nrFinishedTasks = processEngine.getHistoryService().createHistoricTaskInstanceQuery().finished().count();

                System.out.println();
                System.out.println("------------------------------------------------");
                Date timeStamp = new Date();
                System.out.println("Timestamp: " + new Date());
                System.out.println("Nr of process instances = " + nrOfProcessInstances);
                System.out.println("Nr of executions = " + nrOfExecutions);
                System.out.println("Nr of tasks = " + nrOfTasks);
                System.out.println("Nr of async / timer / DL jobs : " + nrOfAsyncJobs + " / " + nrOfTimerJobs + " / " + nrDeadLetterJobs);
                System.out.println("Nr of finished process instances = " + nrFinishedProcessInstances);
                System.out.println("Nr of finished tasks = " + nrFinishedTasks);
                System.out.println("Nr of finished historic activities = " + nrCompletedHistoricActivities);
                System.out.println("------------------------------------------------");
                System.out.println();

                if (printWriter != null) {
                    printWriter.println(timeStamp + ";"
                            + nrOfProcessInstances + ";"
                            + nrOfExecutions + ";"
                            + nrOfTasks + ";"
                            + nrOfAsyncJobs + ";"
                            + nrOfTimerJobs + ";"
                            + nrDeadLetterJobs + ";"
                            + nrFinishedProcessInstances + ";"
                            + nrFinishedTasks + ";"
                            + nrCompletedHistoricActivities);
                    printWriter.flush();
                }

                int expectedProcessInstances = getintProperty("expected-process-instances");
                if (nrFinishedProcessInstances == expectedProcessInstances) {
                    System.out.println("Conditions for stopping are met");
                    allDone = true;
                    processEngine.close();
                }
            }

            if ( numberOfProcessInstances < 0 || (numberOfProcessInstances > 0 && !allDone) ) {
                Thread.sleep(30000L);
            }
        }

        System.out.println("All process instances finished.");

        Date startTime = processEngine.getHistoryService().createHistoricActivityInstanceQuery()
                .orderByHistoricActivityInstanceStartTime().asc().listPage(0, 1).get(0).getStartTime();
        Date endTime = processEngine.getHistoryService().createHistoricActivityInstanceQuery()
                .orderByHistoricActivityInstanceEndTime().desc().listPage(0, 1).get(0).getStartTime();
        long diff = endTime.getTime() - startTime.getTime();
        System.out.println("Time = " + diff + " ms");
        double avg = (double) diff / (double) numberOfProcessInstances;
        System.out.println("Avg time = " + avg + " ms");
        double throughput = 1000.0 / avg;
        System.out.println("Throughput = " + throughput + " process instances / second");

        int nrOfAsyncStepsInProcess = numberOfProcessInstances * 27; // 27 async jobs in process
        System.out.println("Number of executed async jobs = " + nrOfAsyncStepsInProcess);
        double jobsThroughput = 1000.0/ ((double) diff / (double) nrOfAsyncStepsInProcess);
        System.out.println("Throughput = " + jobsThroughput + " jobs / second");

        System.exit(0);
    }

    private static void loadProperties() throws IOException {
        properties = new Properties();
        URL location = Main.class.getProtectionDomain().getCodeSource().getLocation();
        String propertyLocation = location.getPath().replace("asyncexecutor-1.0-SNAPSHOT.jar", "") + "config.properties";
        System.out.println("Reading config from " + propertyLocation);
        FileInputStream fis = new FileInputStream(propertyLocation);
        properties.load(fis);
        System.out.println(properties);
        fis.close();
    }

    private static String getStringProperty(String s) {
        return properties.getProperty(s);
    }

    private static boolean getBooleanProperty(String s) {
        String value = properties.getProperty(s);
        if (value != null) {
            return Boolean.valueOf(value);
        }
        return false;
    }

    private static int getintProperty(String s) {
        String value = properties.getProperty(s);
        if (value != null) {
            return Integer.valueOf(value);
        }
        return -1;
    }

    private static DataSource createDateSource() {

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

    private static void startProcessInstances(final ProcessEngine processEngine) {
        ExecutorService executorService = Executors.newFixedThreadPool(getintProperty("start-process-instance-threads"));
        for (int i=0; i<numberOfProcessInstances; i++) {
            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(delayBetweenProcessInstanceStart);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Map<String, Object> vars = new HashMap<String, Object>();
                    vars.put("input1", random.nextInt(1000));
                    vars.put("input2", random.nextInt(5000));
                    processEngine.getRuntimeService().startProcessInstanceByKey("AsyncProcess", vars);
                }
            });
        }

    }

}
