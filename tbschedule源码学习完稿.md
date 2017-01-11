### 学习下tbschedule

#### 引言
tbschedule是一个分布式的调度框架,其功能的实现主要依赖于zookeeper的强大特性。  
最近正在对zk进行深入学习，拿一个具体框架来印证zk的特性，的确是一个非常不错的决定。
选择tbschedule的原因是在实习公司一个项目可能会用到，所以先了解下。该框架的思想也就是ZK特性
值得学习，源码不敢恭维，当然本屌很菜，可能会有很多不周的地方，坐标来自杭州某重本，本身是个javaer和goper。
闲话不说,我们来解析下这个古老的源码  

#### 如何入手

tbschedule源码提供了test例子，使用war包的确可以，但是不符合现在学习这个的目的。
该例子可以提供我们一定的指示。事实上，如果环境的配置是正确的话，当前你run的时候是可以的。  
先看源码:   

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

代码在这里的阅读性还是比较可观的，一眼即明。
>这个流程也就是这样的    
	1.  首先等待资源初始化，也就是是连上ZK喽    
	2.  清理任务，基于名字爬之前有重名的任务喽(以上两步就是初始化清理)   
	3.  注册任务，在zk上写入相应的信息    
	4.  注册策略，同上  
	5.  被节点读取，操作（tip:在这段代码中你会看不到）  

------
#### 重点的Factory
上面我们说的第5步不在该代码中，那么它是如何实现的呢。  

找到schedule.xml:

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

这里就是标题重要的factory，我们来看一下重点**init**实现。  
zkConfig这个属性倒是明显了(zk信息配置)

    public void init() throws Exception {
		Properties properties = new Properties();
		for(Map.Entry<String,String> e: this.zkConfig.entrySet()){
			properties.put(e.getKey(),e.getValue());
		}
		this.init(properties);
	} 

代码够短，也是很明白的事情，将zkConfig这个map转换成一个Properties对象,然后init调用。  
切换关注点 **this.init(properties)**  

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

为什么要加锁,单例嘛而且内部很多属性都被并发使用到了  
我们之间先看try块中的内容  
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

这里也是很简单的。功能就是连上zk喽。由于采用的是底层的zkAPI所以看起来很是复杂的样子，毕竟开发这个东西的时候，zkClient和Curator之类的东东还不存在么。  

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
作者都自己加了注释了，好明显啊，就是znode信息的初始化，注册调度管理器。  
>首先:zkManager.initial(),在zk上创建顶层zNode顶层节点，并校验zNode里面的信息，是否可用。   
>接着:ScheduleDataManager4ZK(this.zkManager),创建节点，检测。
>然后:ScheduleStrategyDataManager4ZK(this.zkManager)，同上   
>**tip** zk的功能很强大（完全依赖zk）
----
znode层级图

	--/rootPath  
		---/facotry  
			---...尚未创建  
		---/strategy  
			---...尚未创建  
		---/baseTaskType  
			---...尚未创建  

----

接下来的工作依旧是在zNode中创建相应的节点以及信息。   

----

来看Factroy的注册

	public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception{
		
		/**省略相应的代码...功能：创建相应的zNode**/
		
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
为了能在分布式上区分这些节点，作者做在路径上花费了心思，不过一般我们都能想的到。
		
		zNode示意 
			---/facotry  
				---/uuid1(ip1$hostName1$uuid1$seq1)

tip:uuid的解释是本机ip$本机名称$大写去横杆的UUID$zk生成的seq  
tip:这些是临时节点哦

>接着,加载loadAllScheduleStrategy()----------------------doWhat?

>功能：  
	1. 从/strategy这个层级上获得子节点集合/strategyName  
	2. 组装成一个List返回

tip:

>这里的路径就是我们test例子里面。  
>new相应的**任务**对象，**策略**对象。  
>创建zNode路径，并在这些对应路径（zNode）序列化对象数据存储

----

继续看这个for循环，ScheduleStrategy序列化对象，保存了相关分配机器的IP，要特殊处理下。
处理逻辑就是  
那么就应该在zk上删除相关的信心，并把这些ScheduleStrategy的信息返回回来

#### 回头看

接下来是定时任务 

	if(timerTask == null){
			timerTask = new ManagerFactoryTimerTask(this);
			timer.schedule(timerTask, 2000,this.timerInterval);
	}

直接进run中观察吧  

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

看重点:


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

>这里的逻辑也非常的简单，你要确定的是牢记使用zookeeper来观察和记录所有的信息    
>首先看  

	ManagerFactoryInfo stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());

点进去一看果然是这样子的，这个ManegerFactoryInfo存储在znode节点上，到现在为止我们再强势插入一波znode的节点示意图  


	--/rootPath  
		---/facotry  
			---/uuid1(ip1$hostName1$uuid1$seq1):data(ScheduleStrategy类):data(ManagerFactoryInfo类)
			---/uuid2(ip2$hostName2$uuid2$seq2) 
			---/uuid3(ip3$hostName3$uuid3$seq3)
			---...    
		---/strategy  
			---/baseTaskTypeName1$runtime(baseTaskType1的原因是和下面这个是一个名称，$符号是为了分隔环境):data(ScheduleTaskType类)
			---/baseTaskTypeName1$runtime
			---/baseTaskTypeName1$runtime
			---....
		---/baseTaskType  
			---/baseTaskTypeName1(名字了不能含有$符号的哦，因为后期还有$运行环境的各类):data(ScheduleStrategy类)
			---/baseTaskTypeName2
			---/baseTaskTypeName3
			---... 

tip:data表示该层级存储的序列化对象  
>继续返回程序来看整个流程，接下来执行了这个函数:

	public void reRegisterManagerFactory() throws Exception{
		//重新分配调度器
		List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
		this.assignScheduleServer();
		this.reRunScheduleServer();
	}

是不是很熟悉，前面这个函数调用我们已经说过了，这里使用了返回值，来停止Server自然也就明白了，  
我们聚焦**assignScheduleServer()**  

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
代码也很短。这里我们看到了一个新类型啊，**ScheduleStrategyRunntime**，我们先瞧瞧这个是什么吧，它是通过UUID加装下来的，我们看看吧  

	public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(String managerFactoryUUID) throws Exception{
		List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
		String zkPath =	this.PATH_Strategy;
		
		List<String> taskTypeList =  this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(taskTypeList);		
		for(String taskType:taskTypeList){
			if(this.getZooKeeper().exists(zkPath+"/"+taskType+"/"+managerFactoryUUID, false) !=null){				
				result.add(loadScheduleStrategyRunntime(taskType,managerFactoryUUID));
			}
		}
		return result;
	}

看了这个我们的zNode图又要加一个层级了放上喽  

	--/rootPath  
		---/facotry  
			---/uuid1(ip1$hostName1$uuid1$seq1):data(ScheduleStrategy类):data(ManagerFactoryInfo类)
			---/uuid2(ip2$hostName2$uuid2$seq2) 
			---/uuid3(ip3$hostName3$uuid3$seq3)
			---...    
		---/strategy  
			---/baseTaskTypeName1$runtime(baseTaskType1的原因是和下面这个是一个名称，$符号是为了分隔环境):data(ScheduleTaskType类)
				---/uuid_A(和uuid1的结构类型是一样的,也就是某个uuidX):data(ScheduleStrategyRunntime类)
				---/uuid_B
				---/uuid_C
				---...
			---/baseTaskTypeName1$runtime
			---/baseTaskTypeName1$runtime
			---....
		---/baseTaskType  
			---/baseTaskTypeName1(名字了不能含有$符号的哦，因为后期还有$运行环境的各类):data(ScheduleStrategy类)
			---/baseTaskTypeName2
			---/baseTaskTypeName3
			---... 
znode图越完整了，我们继续返回看看整个assignScheduleServer()喽  
在整个for循环中也是很简单的，只用leader才能进行任务的分配，这个leader的策略也是相当的简单啊:
谁的seq最小也就是leader了，这策略十分的简单，而且zk的全局唯一生成的seq还能保证leader是一个    
然后进行任务分配了，分配的思路也很简单啊，每个ScheduleStrategyRunntime类同一个调度策略下（baseTaskTypeName1$runtime）平均分
然后有多的就分上去.  
ex:  
>3台机器，10个东东分配，结果就是 3+1,3,3  
分配好了自然是重新更新下znode上面保存的数据喽   
这样 assignScheduleServer()就完成了。继续调用:  

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

这里也是很简单的，主要就是this.createStrategyTask(strategy);，有多的就挺下来，不够就创建  看一下吧 

	public IStrategyTask createStrategyTask(ScheduleStrategy strategy)
			throws Exception {
		IStrategyTask result = null;
		try{
			if(ScheduleStrategy.Kind.Schedule == strategy.getKind()){
				String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
				String ownSign =ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
				result = new TBScheduleManagerStatic(this,baseTaskType,ownSign,scheduleDataManager);
			}else if(ScheduleStrategy.Kind.Java == strategy.getKind()){
			    result=(IStrategyTask)Class.forName(strategy.getTaskName()).newInstance();
			    result.initialTaskParameter(strategy.getStrategyName(),strategy.getTaskParameter());
			}else if(ScheduleStrategy.Kind.Bean == strategy.getKind()){
			    result=(IStrategyTask)this.getBean(strategy.getTaskName());
			    result.initialTaskParameter(strategy.getStrategyName(),strategy.getTaskParameter());
			}
		}catch(Exception e ){
			logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" +strategy.getStrategyName(),e);
		}
		return result;
	}

现在tbschedule越来越要进来运行的正轨了。这里我们需要关注的只是**TBScheduleManagerStatic**,继续跟踪查看    

	public TBScheduleManagerStatic(TBScheduleManagerFactory aFactory,
			String baseTaskType, String ownSign,IScheduleDataManager aScheduleCenter) throws Exception {
		super(aFactory, baseTaskType, ownSign, aScheduleCenter);
	}

super一下，继续看  

	TBScheduleManager(TBScheduleManagerFactory aFactory,String baseTaskType,String ownSign ,IScheduleDataManager aScheduleCenter) throws Exception{
		this.factory = aFactory;
		this.currentSerialNumber = serialNumber();
		this.scheduleCenter = aScheduleCenter;
		this.taskTypeInfo = this.scheduleCenter.loadTaskTypeBaseInfo(baseTaskType);
    	log.info("create TBScheduleManager for taskType:"+baseTaskType);
		//清除已经过期1天的TASK,OWN_SIGN的组合。超过一天没有活动server的视为过期
		this.scheduleCenter.clearExpireTaskTypeRunningInfo(baseTaskType,ScheduleUtil.getLocalIP() + "清除过期OWN_SIGN信息",this.taskTypeInfo.getExpireOwnSignInterval());

		//校验并得到bean
		Object dealBean = aFactory.getBean(this.taskTypeInfo.getDealBeanName());
		if (dealBean == null) {
			throw new Exception( "SpringBean " + this.taskTypeInfo.getDealBeanName() + " 不存在");
		}
		if (dealBean instanceof IScheduleTaskDeal == false) {
			throw new Exception( "SpringBean " + this.taskTypeInfo.getDealBeanName() + " 没有实现 IScheduleTaskDeal接口");
		}
    	this.taskDealBean = (IScheduleTaskDeal)dealBean;

		//配置ScheduleTaskType的校验
    	if(this.taskTypeInfo.getJudgeDeadInterval() < this.taskTypeInfo.getHeartBeatRate() * 5){
    		throw new Exception("数据配置存在问题，死亡的时间间隔，至少要大于心跳线程的5倍。当前配置数据：JudgeDeadInterval = "
    				+ this.taskTypeInfo.getJudgeDeadInterval() 
    				+ ",HeartBeatRate = " + this.taskTypeInfo.getHeartBeatRate());
    	}

		//向zk注册ScheduleServer
    	this.currenScheduleServer = ScheduleServer.createScheduleServer(this.scheduleCenter,baseTaskType,ownSign,this.taskTypeInfo.getThreadNumber());
    	this.currenScheduleServer.setManagerFactoryUUID(this.factory.getUuid());
    	scheduleCenter.registerScheduleServer(this.currenScheduleServer);
    	this.mBeanName = "pamirs:name=" + "schedule.ServerMananger." +this.currenScheduleServer.getUuid();
    	this.heartBeatTimer = new Timer(this.currenScheduleServer.getTaskType() +"-" + this.currentSerialNumber +"-HeartBeat");
    	this.heartBeatTimer.schedule(new HeartBeatTimerTask(this),
                new java.util.Date(System.currentTimeMillis() + 500),
                this.taskTypeInfo.getHeartBeatRate());
    	initial();
	}  

代码依旧不是很长，我们稍微详细来介绍下。就是设置相应的属性,比如factory啊，currentSerialNumber是一个线程标示，数据中心啊，任务信息啊，  
然后清理了过期的server  
从任务信息得到相应的处理bean，这就是前面为什么名称是一样的原因了，  
在就是校验下任务的配置是否合法喽   

接下来是比较重要的操作了，首先是生成一个ScheduleServer,   
然后向znode某个路径注册了当前currenScheduleServer(这个东西十分的重要喽) 
然后znode层级发生了变化 现在的层级是这样的  

---
>Znode示意图:	
	--/rootPath  
		---/facotry  
			---/uuid1(ip1$hostName1$uuid1$seq1):data(ScheduleStrategy类):data(ManagerFactoryInfo类)
			---/uuid2(ip2$hostName2$uuid2$seq2) 
			---/uuid3(ip3$hostName3$uuid3$seq3)
			---...    
		---/strategy  
			---/baseTaskTypeName1$runtime(baseTaskType1的原因是和下面这个是一个名称，$符号是为了分隔环境):data(ScheduleTaskType类)
				---/uuid_A(和uuid1的结构类型是一样的,也就是某个uuidX):data(ScheduleStrategyRunntime类)
				---/uuid_B
				---/uuid_C
				---...
			---/baseTaskTypeName2$runtime
			---/baseTaskTypeName3$runtime
			---....
		---/baseTaskType  
			---/baseTaskTypeName1(名字了不能含有$符号的哦，因为后期还有$运行环境的各类):data(ScheduleStrategy类)
				---/baseTaskTypeName1$runtime1(可能也只是"BASE"一般我们不会这样的)
					---/server
						---/baseTaskTypeName1$runtime1$ip$uuid$seq(用来标示server的):data(ScheduleServer类)
				---/baseTaskTypeName1$runtime2
				---/baseTaskTypeName1$runtime3
				---...
			---/baseTaskTypeName2
			---/baseTaskTypeName3
			---... 

回到程序继续看接下是一个心跳监测，先略过,点进去也没用，没有初始化完成之前，会被直接返回的。
我们先看initial()  

	public void initial() throws Exception{
    	new Thread(this.currenScheduleServer.getTaskType()  +"-" + this.currentSerialNumber +"-StartProcess"){
    		@SuppressWarnings("static-access")
			public void run(){
    			try{
    			   log.info("开始获取调度任务队列...... of " + currenScheduleServer.getUuid());
					//由leader负责初始化，并会在taskItem计入leader，其他节点，会等待leader的初始化完毕
    			   while (isRuntimeInfoInitial == false) {
 				      if(isStopSchedule == true){
				    	  log.debug("外部命令终止调度,退出调度队列获取：" + currenScheduleServer.getUuid());
				    	  return;
				      }
 				      //log.error("isRuntimeInfoInitial = " + isRuntimeInfoInitial);
 				      try{
					  initialRunningInfo();
					  isRuntimeInfoInitial = scheduleCenter.isInitialRunningInfoSucuss(
										currenScheduleServer.getBaseTaskType(),
										currenScheduleServer.getOwnSign());
 				      }catch(Throwable e){
 				    	  //忽略初始化的异常
 				    	  log.error(e.getMessage(),e);
 				      }
					  if(isRuntimeInfoInitial == false){
    				      Thread.currentThread().sleep(1000);
					  }
					}

    			   int count =0;
    			   lastReloadTaskItemListTime = scheduleCenter.getSystemTime();
				   while(getCurrentScheduleTaskItemListNow().size() <= 0){
    				      if(isStopSchedule == true){
    				    	  log.debug("外部命令终止调度,退出调度队列获取：" + currenScheduleServer.getUuid());
    				    	  return;
    				      }
    				      Thread.currentThread().sleep(1000);
        			      count = count + 1;
        			     // log.error("尝试获取调度队列，第" + count + "次 ") ;
    			   }
    			   String tmpStr ="TaskItemDefine:";
    			   for(int i=0;i< currentTaskItemList.size();i++){
    				   if(i>0){
    					   tmpStr = tmpStr +",";    					   
    				   }
    				   tmpStr = tmpStr + currentTaskItemList.get(i);
    			   }
    			   log.info("获取到任务处理队列，开始调度：" + tmpStr +"  of  "+ currenScheduleServer.getUuid());
    			   
    		    	//任务总量
    		    	taskItemCount = scheduleCenter.loadAllTaskItem(currenScheduleServer.getTaskType()).size();
    		    	//只有在已经获取到任务处理队列后才开始启动任务处理器    			   
    			   computerStart();
    			}catch(Exception e){
    				log.error(e.getMessage(),e);
    				String str = e.getMessage();
    				if(str.length() > 300){
    					str = str.substring(0,300);
    				}
    				startErrorInfo = "启动处理异常：" + str;
    			}
    		}
    	}.start();
    }

代码稍微有点长，首先是一部分的while这里的功能是等待初始化完成，如何初始化完成的呢，  
这是由leader来负责的，其他都会在这里进行死循环，心跳线程也会是一个死任务，当主leader进来之后  
执行**initialRunningInfo();**进去看一下吧 


	public void initialRunningInfo() throws Exception{
		scheduleCenter.clearExpireScheduleServer(this.currenScheduleServer.getTaskType(),this.taskTypeInfo.getJudgeDeadInterval());
		List<String> list = scheduleCenter.loadScheduleServerNames(this.currenScheduleServer.getTaskType());
		if(scheduleCenter.isLeader(this.currenScheduleServer.getUuid(),list)){
	    	//是第一次启动，先清楚所有的垃圾数据
			log.debug(this.currenScheduleServer.getUuid() + ":" + list.size());
	    	this.scheduleCenter.initialRunningInfo4Static(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getOwnSign(),this.currenScheduleServer.getUuid());
	    }
	 }
清楚功能就不看了，由leader来负责，直接看重点

	public void initialRunningInfo4Static(String baseTaskType, String ownSign,String uuid)
			throws Exception {
		  
		 String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
		 //清除所有的老信息，只有leader能执行此操作
		 String zkPath = this.PATH_BaseTaskType+"/"+ baseTaskType +"/" + taskType+"/" + this.PATH_TaskItem;
		 try {
			 ZKTools.deleteTree(this.getZooKeeper(),zkPath);
		 } catch (Exception e) {
				//需要处理zookeeper session过期异常
				if (e instanceof KeeperException
						&& ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED.intValue()) {
					log.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
					zkManager.reConnection();
					ZKTools.deleteTree(this.getZooKeeper(),zkPath);
				}
		 }
		 //创建目录
		 this.getZooKeeper().create(zkPath,null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
		 //创建静态任务
		 this.createScheduleTaskItem(baseTaskType, ownSign,this.loadTaskTypeBaseInfo(baseTaskType).getTaskItems());
		 //标记信息初始化成功
		 setInitialRunningInfoSucuss(baseTaskType,taskType,uuid);
	}
一眼望去又涉及到znode，又有一个层级进来了，我们按照之前所说的，先补充znode图  

	--/rootPath  
		---/facotry  
			---/uuid1(ip1$hostName1$uuid1$seq1):data(ScheduleStrategy类):data(ManagerFactoryInfo类)
			---/uuid2(ip2$hostName2$uuid2$seq2) 
			---/uuid3(ip3$hostName3$uuid3$seq3)
			---...    
		---/strategy  
			---/baseTaskTypeName1$runtime(baseTaskType1的原因是和下面这个是一个名称，$符号是为了分隔环境):data(ScheduleTaskType类)
				---/uuid_A(和uuid1的结构类型是一样的,也就是某个uuidX):data(ScheduleStrategyRunntime类)
				---/uuid_B
				---/uuid_C
				---...
			---/baseTaskTypeName2$runtime
			---/baseTaskTypeName3$runtime
			---....
		---/baseTaskType  
			---/baseTaskTypeName1(名字了不能含有$符号的哦，因为后期还有$运行环境的各类):data(ScheduleStrategy类)
				---/baseTaskTypeName1$runtime1(可能也只是"BASE"一般我们不会这样的)
					---/server
						---/baseTaskTypeName1$runtime1$ip$uuid$seq(用来标示server的):data(ScheduleServer类)-----mark
					---/taskItem:data(ScheduleServer持有的UUID也就是mark这里的标记)
						---/序号1
							---/cur_server(当前server)
							---/req_server(next的server)
							---/sts(状态:active,finish,halt)
							---/parameter(任务处理需要的参数)
							---/deal_desc(任务处理情况,用于任务处理器会写一些信息,描述信息喽)
						---/序号2
						---/序号3
						---...
				---/baseTaskTypeName1$runtime2
				---/baseTaskTypeName1$runtime3
				---...
			---/baseTaskTypeName2
			---/baseTaskTypeName3
			---... 
>补充完毕，我们来说一下，上面就是由leader来创建znode的任务路径也就是**taskItem**  
>接着就是在创建相应的具体的调度任务队列，然后在该路径下创建具体的任务队列信息,也就是上面的的taskItem的子路径，  
>每一个序号就是一个队列  
>然后写入相应的信息(子路径中)  
>全部完成后再路径**taskItem**写入leader的UUID表示运行时的初始化完毕了   
>**tip** : 这里的leader指的是ScheduleServer。选举算法也是seq最小    


----

当初始化完毕之后，其他节点，已经leader节点的initail和心跳过程都能顺利向下执行了  
也就是说初始化完毕，可以正式干正事了    

现在就是任务队列的分配了，也是leader负责的，看leader首先设置成功标志，然后清除任务了  
即clearTaskItem(),值得说明的是为什么里面find呢，因为要么都是，要么没有  
然后assignTaskItem。这是由leader安排的  
这里面的逻辑有点复杂啊，其实也不复杂，就是设置相应的信息喽     
另外一边在初始化过程中要获取当前的队列   这是一个加锁的方法  
先做了一步检测僵尸进程的检查  
接下来申请队列  
然后清理当前的队列  
最后中心加载队列  
获取之后开始

----

最后执行computerStart()  

	/**
	 * 开始的时候，计算第一次执行时间
	 * @throws Exception
	 */
    public void computerStart() throws Exception{
    	//只有当存在可执行队列后再开始启动队列
   	
    	boolean isRunNow = false;
    	if(this.taskTypeInfo.getPermitRunStartTime() == null){
    		isRunNow = true;
    	}else{
    		String tmpStr = this.taskTypeInfo.getPermitRunStartTime();
			if(tmpStr.toLowerCase().startsWith("startrun:")){
				isRunNow = true;
				tmpStr = tmpStr.substring("startrun:".length());
	    	}

			CronExpression cexpStart = new CronExpression(tmpStr);
    		Date current = new Date( this.scheduleCenter.getSystemTime());
    		Date firstStartTime = cexpStart.getNextValidTimeAfter(current);

    		this.heartBeatTimer.schedule(
    				new PauseOrResumeScheduleTask(this,this.heartBeatTimer,
    						PauseOrResumeScheduleTask.TYPE_RESUME,tmpStr), 
    						firstStartTime);
			this.currenScheduleServer.setNextRunStartTime(ScheduleUtil.transferDataToString(firstStartTime));	
			if( this.taskTypeInfo.getPermitRunEndTime() == null
    		   || this.taskTypeInfo.getPermitRunEndTime().equals("-1")){
				this.currenScheduleServer.setNextRunEndTime("当不能获取到数据的时候pause");				
			}else{
				try {
					String tmpEndStr = this.taskTypeInfo.getPermitRunEndTime();
					CronExpression cexpEnd = new CronExpression(tmpEndStr);
					Date firstEndTime = cexpEnd.getNextValidTimeAfter(firstStartTime);
					Date nowEndTime = cexpEnd.getNextValidTimeAfter(current);
					if(!nowEndTime.equals(firstEndTime) && current.before(nowEndTime)){
						isRunNow = true;
						firstEndTime = nowEndTime;
					}
					this.heartBeatTimer.schedule(
		    				new PauseOrResumeScheduleTask(this,this.heartBeatTimer,
		    						PauseOrResumeScheduleTask.TYPE_PAUSE,tmpEndStr), 
		    						firstEndTime);
					this.currenScheduleServer.setNextRunEndTime(ScheduleUtil.transferDataToString(firstEndTime));
				} catch (Exception e) {
					log.error("计算第一次执行时间出现异常:" + currenScheduleServer.getUuid(), e);
					throw new Exception("计算第一次执行时间出现异常:" + currenScheduleServer.getUuid(), e);
				}
			}
    	}
    	if(isRunNow == true){
    		this.resume("开启服务立即启动");
    	}
    	this.rewriteScheduleInfo();
    	
    }  

以cratob的风格设置相应的运行，停止阶段，停止的时候停止调度。运行时间段开始运行  
最后首先开始执行，最关键的下面函数  

	public void resume(String message) throws Exception{
		if (this.isPauseSchedule == true) {
			if(log.isDebugEnabled()){
				log.debug("恢复调度:" + this.currenScheduleServer.getUuid());
			}
			this.isPauseSchedule = false;
			this.pauseMessage = message;
			if (this.taskDealBean != null) {
				if (this.taskTypeInfo.getProcessorType() != null &&
					this.taskTypeInfo.getProcessorType().equalsIgnoreCase("NOTSLEEP")==true){
					this.taskTypeInfo.setProcessorType("NOTSLEEP");
					this.processor = new TBScheduleProcessorNotSleep(this,
							taskDealBean,this.statisticsInfo);
				}else{
					this.processor = new TBScheduleProcessorSleep(this,
							taskDealBean,this.statisticsInfo);
					this.taskTypeInfo.setProcessorType("SLEEP");
				}
			}
			rewriteScheduleInfo();
		}
	}	


值得注意的是，在上面的PasueOrResume也可能会执行这个的。
现在执行，采用了两种策略，一种是NotSleep，另外一种是sleep，两个都需要注意，先看notSleep    

	/**
	 * 运行函数
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		long startTime = 0;
		long sequence = 0;
		Object executeTask = null;	
		while (true) {
			try {
				if (this.isStopSchedule == true) { // 停止队列调度
					synchronized (this.threadList) {
						this.threadList.remove(Thread.currentThread());
						if(this.threadList.size()==0){
							this.scheduleManager.unRegisterScheduleServer();
						}
					}
					return;
				}
				// 加载调度任务
				if (this.isMutilTask == false) {
					executeTask = this.getScheduleTaskId();
				} else {
					executeTask = this.getScheduleTaskIdMulti();
				}
				if (executeTask == null ) {
					this.loadScheduleData();
					continue;
				}
				
				try { // 运行相关的程序
					this.runningTaskList.add(executeTask);
					startTime = scheduleManager.scheduleCenter.getSystemTime();
					sequence = sequence + 1;
					if (this.isMutilTask == false) {
						if (((IScheduleTaskDealSingle<Object>) this.taskDealBean).execute(executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
							addSuccessNum(1, scheduleManager.scheduleCenter.getSystemTime()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
						} else {
							addFailNum(1,scheduleManager.scheduleCenter.getSystemTime()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
						}
					} else {
						if (((IScheduleTaskDealMulti<Object>) this.taskDealBean)
								.execute((Object[]) executeTask,scheduleManager.getScheduleServer().getOwnSign()) == true) {
							addSuccessNum(((Object[]) executeTask).length, scheduleManager.scheduleCenter.getSystemTime()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
						} else {
							addFailNum(((Object[]) executeTask).length, scheduleManager.scheduleCenter.getSystemTime()
									- startTime,
									"com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
						}
					}
				} catch (Throwable ex) {
					if (this.isMutilTask == false) {
						addFailNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
								"TBScheduleProcessor.run");
					} else {
						addFailNum(((Object[]) executeTask).length, scheduleManager.scheduleCenter.getSystemTime()
								- startTime,
								"TBScheduleProcessor.run");
					}
					logger.error("Task :" + executeTask + " 处理失败", ex);
				} finally {
					this.runningTaskList.remove(executeTask);
				}
			} catch (Throwable e) {
				throw new RuntimeException(e);
				//log.error(e.getMessage(), e);
			}
		}
	}


获取相关的数据**this.loadScheduleData();**    



		/**
		* 装载数据
		* @return
		*/
		protected int loadScheduleData() {
			lockLoadData.lock();
			try {
				if (this.taskList.size() > 0 || this.isStopSchedule == true) { // 判断是否有别的线程已经装载过了。
					return this.taskList.size();
				}
				// 在每次数据处理完毕后休眠固定的时间
				try {
					if (this.taskTypeInfo.getSleepTimeInterval() > 0) {
						if (logger.isTraceEnabled()) {
							logger.trace("处理完一批数据后休眠："
									+ this.taskTypeInfo.getSleepTimeInterval());
						}
						this.isSleeping = true;
						Thread.sleep(taskTypeInfo.getSleepTimeInterval());
						this.isSleeping = false;
					
					if (logger.isTraceEnabled()) {
						logger.trace("处理完一批数据后休眠后恢复");
					}
				}
			} catch (Throwable ex) {
				logger.error("休眠时错误", ex);
			}

			putLastRunningTaskList();// 将running队列的数据拷贝到可能重复的队列中

			try {
				List<TaskItemDefine> taskItems = this.scheduleManager
						.getCurrentScheduleTaskItemList();
				// 根据队列信息查询需要调度的数据，然后增加到任务列表中
				if (taskItems.size() > 0) {
					List<TaskItemDefine> tmpTaskList= new ArrayList<TaskItemDefine>();
					synchronized(taskItems){
						for (TaskItemDefine taskItemDefine : taskItems) {
							tmpTaskList.add(taskItemDefine);
						}
					}
					List<T> tmpList = this.taskDealBean.selectTasks(
							taskTypeInfo.getTaskParameter(),
							scheduleManager.getScheduleServer()
									.getOwnSign(), this.scheduleManager.getTaskItemCount(), tmpTaskList,
							taskTypeInfo.getFetchDataNumber());
					scheduleManager.getScheduleServer().setLastFetchDataTime(new Timestamp(scheduleManager.scheduleCenter.getSystemTime()));
					if (tmpList != null) {
						this.taskList.addAll(tmpList);
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug("没有任务分配");
					}
				}
				addFetchNum(taskList.size(),
						"TBScheduleProcessor.loadScheduleData");
				if (taskList.size() <= 0) {
					// 判断当没有数据的是否，是否需要退出调度
					if (this.scheduleManager.isContinueWhenData() == true) {
						if (taskTypeInfo.getSleepTimeNoData() > 0) {
							if (logger.isDebugEnabled()) {
								logger.debug("没有读取到需要处理的数据,sleep "
										+ taskTypeInfo.getSleepTimeNoData());
							}
							this.isSleeping = true;
							Thread.sleep(taskTypeInfo.getSleepTimeNoData());
							this.isSleeping = false;							
						}
					}
				}
				return this.taskList.size();
			} catch (Throwable ex) {
				logger.error("获取任务数据错误", ex);
			}
			return 0;
		} finally {
			lockLoadData.unlock();
		}
	}

这个做的也很简单要是干了什么事情，就直接返回  
如果任务设置有睡觉则睡觉一会   
然后把将running队列的数据拷贝到可能重复的队列中  

然后加载相关的队列信息   

	 List<TaskItemDefine> getCurrentScheduleTaskItemList()  

	/**
	 * 重新加载当前服务器的任务队列
	 * 1、释放当前服务器持有，但有其它服务器进行申请的任务队列
	 * 2、重新获取当前服务器的处理队列
	 * 
	 * 为了避免此操作的过度，阻塞真正的数据处理能力。系统设置一个重新装载的频率。例如1分钟
	 * 
	 * 特别注意：
	 *   此方法的调用必须是在当前所有任务都处理完毕后才能调用，否则是否任务队列后可能数据被重复处理
	 */  

功能是这样的    
接着根据队列信息查询需要调度的数据，然后增加到任务列表中  
执行相应的select的操作，然后把数据放置在taskList中  
这样加载数据就完成了

回到NotSleep中 接下来就是执行executeTask了根据相应的标志设置
转化为相应的接口去执行execute方法  
执行成功后就不要这个队列了。
到此完成整个源码阅读 然后不断重复这个操作


