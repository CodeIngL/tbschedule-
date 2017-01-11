### 学习下tbschedule

tbschedule是一个分布式的调度框架，我们来解析下源码  

#### 如何入手

tbschedule源码提供了一test文件，这个文件可以提供我们一定的指示  


    @Test
	public void initialConfigData() throws Exception {
		String baseTaskTypeName = "DemoTask";
		while(this.scheduleManagerFactory.isZookeeperInitialSucess() == false){
			Thread.sleep(1000);
		}
		scheduleManagerFactory.stopServer(null);
		Thread.sleep(1000);
		try {
			this.scheduleManagerFactory.getScheduleDataManager()
					.deleteTaskType(baseTaskTypeName);
		} catch (Exception e) {
		}

		// 创建任务调度DemoTask的基本信息
		ScheduleTaskType baseTaskType = new ScheduleTaskType();
		baseTaskType.setBaseTaskType(baseTaskTypeName);
		baseTaskType.setDealBeanName("demoTaskBean");
		baseTaskType.setHeartBeatRate(2000);
		baseTaskType.setJudgeDeadInterval(10000);
		baseTaskType.setTaskParameter("AREA=杭州,YEAR>30");
		baseTaskType.setTaskItems(ScheduleTaskType.splitTaskItem(
				"0:{TYPE=A,KIND=1},1:{TYPE=A,KIND=2},2:{TYPE=A,KIND=3},3:{TYPE=A,KIND=4}," +
				"4:{TYPE=A,KIND=5},5:{TYPE=A,KIND=6},6:{TYPE=A,KIND=7},7:{TYPE=A,KIND=8}," +
				"8:{TYPE=A,KIND=9},9:{TYPE=A,KIND=10}"));
		this.scheduleManagerFactory.getScheduleDataManager()
				.createBaseTaskType(baseTaskType);
		log.info("创建调度任务成功:" + baseTaskType.toString());

		// 创建任务DemoTask的调度策略
		String taskName = baseTaskTypeName + "$TEST";
		String strategyName = baseTaskTypeName +"-Strategy";
		try {
			this.scheduleManagerFactory.getScheduleStrategyManager()
					.deleteMachineStrategy(strategyName,true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		ScheduleStrategy strategy = new ScheduleStrategy();
		strategy.setStrategyName(strategyName);
		strategy.setKind(ScheduleStrategy.Kind.Schedule);
		strategy.setTaskName(taskName);
		strategy.setTaskParameter("中国");
		
		strategy.setNumOfSingleServer(1);
		strategy.setAssignNum(10);
		strategy.setIPList("127.0.0.1".split(","));
		this.scheduleManagerFactory.getScheduleStrategyManager()
				.createScheduleStrategy(strategy);
		log.info("创建调度策略成功:" + strategy.toString());

	}

代码有点短，关注点在**scheduleManagerFactory**上面，这个是一个bean在schedule.xml中配置，配置如下:

    <bean id="scheduleManagerFactory" class="com.taobao.pamirs.schedule.strategy.TBScheduleManagerFactory"
		init-method="init">
		<property name="zkConfig">
           <map>
              <entry key="zkConnectString" value="localhost:2181" />
              <entry key="rootPath" value="/taobao-pamirs-schedule/xuannan" />
              <entry key="zkSessionTimeout" value="60000" />
              <entry key="userName" value="ScheduleAdmin" />
              <entry key="password" value="password" />
              <entry key="isCheckParentPath" value="true" />
           </map>
       </property>	
	</bean> 

切换关注点init-method的**init**实现:其来源于**scheduleManagerFactory**的init()方法  
至于zkConfig说的很明显了,关于zk的配置,现在先来看一下init()方法

    public void init() throws Exception {
		Properties properties = new Properties();
		for(Map.Entry<String,String> e: this.zkConfig.entrySet()){
			properties.put(e.getKey(),e.getValue());
		}
		this.init(properties);
	} 

功能很简单，将zkConfig这个map转换成一个Properties对象,然后init调用。继续切换关注点 **this.init(properties)**


    public void init(Properties p) throws Exception {
	    if(this.initialThread != null){
	    	this.initialThread.stopThread();
	    }
		this.lock.lock();
		try{
			this.scheduleDataManager = null;
			this.scheduleStrategyManager = null;
		    ConsoleManager.setScheduleManagerFactory(this);
		    if(this.zkManager != null){
				this.zkManager.close();
			}
			this.zkManager = new ZKManager(p);
			this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
			initialThread = new InitialThread(this);
			initialThread.setName("TBScheduleManagerFactory-initialThread");
			initialThread.start();
		}finally{
			this.lock.unlock();
		}
	}

为什么加锁,单例嘛，而且内部很多属性都被并发使用到了  
上面的挂掉的先忽略吧，我们之间看try块中的内容  
>ConsoleManager是一个工具类。没有多大的关注点，里面也就是简单将工厂设置进CosoleManager,  
>等我们要用到的时候回来看      
>然后就是一些清理的东西,然后根据zk配置来重新配置zkManager我们看一下这个里面做了什么呢  

	public ZKManager(Properties aProperties) throws Exception{
		this.properties = aProperties;
		this.connect();
	}
将Propeties设置先，然后准备连接，进入源码继续看   

	private void connect() throws Exception {
		CountDownLatch connectionLatch = new CountDownLatch(1);
		createZookeeper(connectionLatch);
		connectionLatch.await(10,TimeUnit.SECONDS);
	}
createZookeeper()关注点    

	private void createZookeeper(final CountDownLatch connectionLatch) throws Exception {
		zk = new ZooKeeper(this.properties.getProperty(keys.zkConnectString
				.toString()), Integer.parseInt(this.properties
				.getProperty(keys.zkSessionTimeout.toString())),
				new Watcher() {
					public void process(WatchedEvent event) {
						sessionEvent(connectionLatch, event);
					}
				});
		String authString = this.properties.getProperty(keys.userName.toString())
				+ ":"+ this.properties.getProperty(keys.password.toString());
		this.isCheckParentPath = Boolean.parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(),"true"));
		log.info("disgest----------------------------"+authString);
		zk.addAuthInfo("digest", authString.getBytes());
		acl.clear();
		acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
				DigestAuthenticationProvider.generateDigest(authString))));
		acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
	}

这里也是很简单的

>然后初始化一个线程，这个线程就是我们之前先忽略的东西，现在我们来重新看一下，这个线程 

详细介绍下，  
 >**initialThread**线程  
  
  	@Override
	public void run() {
		facotry.lock.lock();
		try {
			int count =0;
			while(facotry.zkManager.checkZookeeperState() == false){
				count = count + 1;
				if(count % 50 == 0){
					facotry.errorMessage = "Zookeeper connecting ......" + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20 +"(ms)";
					log.error(facotry.errorMessage);
				}
				Thread.sleep(20);
				if(this.isStop ==true){
					return;
				}
			}
			facotry.initialData();
		} catch (Throwable e) {
			 log.error(e.getMessage(),e);
		}finally{
			facotry.lock.unlock();
		}
	}
作用就是,zk出现问题，不能连接的时候，1000ms之内有问题，就记录错误楼。这不是重点  
重点是**initialData()** 

根据名称应该可以猜测出一些东西的咯  进入看一看   

    /**
     * 在Zk状态正常后回调数据初始化
     * @throws Exception
     */
	public void initialData() throws Exception{
			this.zkManager.initial();
			this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
			this.scheduleStrategyManager  = new ScheduleStrategyDataManager4ZK(this.zkManager);
			if (this.start == true) {
				// 注册调度管理器
				this.scheduleStrategyManager.registerManagerFactory(this);
				if(timer == null){
					timer = new Timer("TBScheduleManagerFactory-Timer");
				}
				if(timerTask == null){
					timerTask = new ManagerFactoryTimerTask(this);
					timer.schedule(timerTask, 2000,this.timerInterval);
				}
			}
	}
作者都自己加了注释了，好明显啊，在我们下一步之前，我们应该会过头来看看  
    
    public ZKManager(Properties aProperties) throws Exception{
            this.properties = aProperties;
            this.connect();
    }

作者在这个之前做过连接的，然后我们看一下连接里面做了 createZookeeper    

	    zk = new ZooKeeper(this.properties.getProperty(keys.zkConnectString
				.toString()), Integer.parseInt(this.properties
				.getProperty(keys.zkSessionTimeout.toString())),
				new Watcher() {
					public void process(WatchedEvent event) {
						sessionEvent(connectionLatch, event);
					}
				});
		String authString = this.properties.getProperty(keys.userName.toString())
				+ ":"+ this.properties.getProperty(keys.password.toString());
		this.isCheckParentPath = Boolean.parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(),"true"));
		zk.addAuthInfo("digest", authString.getBytes());
		acl.clear();
		acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
				DigestAuthenticationProvider.generateDigest(authString))));
		acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));

整体上就是连上zk。我们规定都是假设成功，那么现在我们又要跳动，来观察一些东西了。实现看到上面的**initialData()**  
现在就是数据的初始化了。把正常的东西全都初始化话好，然后注册调度管理器。这里我们直接考虑的是调度，不是系统  

>调度管理 我们需要重新看一下  
 
 	/**
	 * 注册ManagerFactory
	 * @param managerFactory
	 * @return 需要全部注销的调度，例如当IP不在列表中
	 * @throws Exception
	 */
	public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception{
		
		if(managerFactory.getUuid() == null){
			String uuid = managerFactory.getIp() +"$" + managerFactory.getHostName() +"$"+ UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
			String zkPath = this.PATH_ManagerFactory + "/" + uuid +"$";
			zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
			managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
		}else{
			String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
			if(this.getZooKeeper().exists(zkPath, false)==null){
				zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);			
			}
		}
		
		List<String> result = new ArrayList<String>();
		for(ScheduleStrategy scheduleStrategy:loadAllScheduleStrategy()){
			boolean isFind = false;
			//暂停或者不在IP范围
			if(ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) == false &&  scheduleStrategy.getIPList() != null){
				for(String ip:scheduleStrategy.getIPList()){
					if(ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp())|| ip.equalsIgnoreCase(managerFactory.getHostName())){
						//添加可管理TaskType
						String zkPath =	this.PATH_Strategy+"/"+ scheduleStrategy.getStrategyName()+ "/"+ managerFactory.getUuid();
						if(this.getZooKeeper().exists(zkPath, false)==null){
							zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);			
						}
						isFind = true;
						break;
					}
				}
			}
			if(isFind == false){//清除原来注册的Factory
				String zkPath =	this.PATH_Strategy+"/"+ scheduleStrategy.getStrategyName()+ "/"+ managerFactory.getUuid();
				if(this.getZooKeeper().exists(zkPath, false)!=null){
					ZKTools.deleteTree(this.getZooKeeper(), zkPath);
					result.add(scheduleStrategy.getStrategyName());
				}
			}
		}
		return result;
	}

这里是干嘛来着的呢。我们可以看一下 首先根据本机的信息组成一个路径。

> this.PATH_ManagerFactory+"/ip$hostName$+随机的uuid去掉-的小写+$"  
这个PATH_ManagerFatory是什么呢 。我们看一下 
默认是${rootPath}/factory;  
其中${XXX}标示来自配置文件   
综上 路径是${rootPath}/factory/ip$hostName$uuid$  
然后创建zk节点。然后managerFactory保存自己的uuid，这相当于自己的信息  

>接着,加载loadAllScheduleStrategy()怎么做的呢


    public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
		String zkPath = this.PATH_Strategy;
		List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
		List<String> names = this.getZooKeeper().getChildren(zkPath,false);
		Collections.sort(names);
		for(String name:names){
			result.add(this.loadStrategy(name));
		}
		return result;
	}

zkPath是${rootPath}/strategy
result是其下所有的节点。
然后将集群下的重新添加相应的TaskType。将他变成zk节点  

然后就注册完毕 
回过头来继续看啊 

定时调度    

	    if(timerTask == null){
					timerTask = new ManagerFactoryTimerTask(this);
					timer.schedule(timerTask, 2000,this.timerInterval);
		}

---

    public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			if(this.factory.zkManager.checkZookeeperState() == false){
				if(count > 5){
				   log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
				   this.factory.reStart();
				  
				}else{
				   count = count + 1;
				}
			}else{
				count = 0;
			    this.factory.refresh();
			}

		}  catch (Throwable ex) {
			log.error(ex.getMessage(), ex);
		} finally {
		    factory.timerTaskHeartBeatTS = System.currentTimeMillis();
		}
	}

干了什么呢  ManagerFatory刷新  


    public void refresh() throws Exception {
		this.lock.lock();
		try {
			// 判断状态是否终止
			ManagerFactoryInfo stsInfo = null;
			boolean isException = false;
			try {
				stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
			} catch (Exception e) {
				isException = true;
				logger.error("获取服务器信息有误：uuid="+this.getUuid(), e);
			}
			if (isException == true) {
				try {
					stopServer(null); // 停止所有的调度任务
					this.getScheduleStrategyManager().unRregisterManagerFactory(this);
				} finally {
					reRegisterManagerFactory();
				}
			} else if (stsInfo.isStart() == false) {
				stopServer(null); // 停止所有的调度任务
				this.getScheduleStrategyManager().unRregisterManagerFactory(
						this);
			} else {
				reRegisterManagerFactory();
			}
		} finally {
			this.lock.unlock();
		}
	}

---

	public void reRegisterManagerFactory() throws Exception{
		//重新分配调度器
		List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
		this.assignScheduleServer();
		this.reRunScheduleServer();
	}

--- 

	/**
	 * 根据策略重新分配调度任务的机器
	 * @throws Exception
	 */
	public void assignScheduleServer() throws Exception{
		for(ScheduleStrategyRunntime run: this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)){
			List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
			if(factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) ==false){
				continue;
			}
			ScheduleStrategy scheduleStrategy =this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			
			int[] nums =  ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
			for(int i=0;i<factoryList.size();i++){
				ScheduleStrategyRunntime factory = 	factoryList.get(i);
				//更新请求的服务器数量
				this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(), 
						factory.getUuid(),nums[i]);
			}
		}
	}

--- 

	public void reRunScheduleServer() throws Exception{
		for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
			List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
			if(list == null){
				list = new ArrayList<IStrategyTask>();
				this.managerMap.put(run.getStrategyName(),list);
			}
			while(list.size() > run.getRequestNum() && list.size() >0){
				IStrategyTask task  =  list.remove(list.size() - 1);
					try {
						task.stop(run.getStrategyName());
					} catch (Throwable e) {
						logger.error("注销任务错误：strategyName=" + run.getStrategyName(), e);
					}
				}
		   //不足，增加调度器
		   ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
		   while(list.size() < run.getRequestNum()){
			   IStrategyTask result = this.createStrategyTask(strategy);
			   if(null==result){
				   logger.error("strategy 对应的配置有问题。strategy name="+strategy.getStrategyName());
			   }
			   list.add(result);
		    }
		}
	}

---