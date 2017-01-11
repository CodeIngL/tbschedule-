### 学习下tbschedule

tbschedule是一个分布式的调度框架，其功能的实现主要依赖于zookeeper，最近正在对zk进行深入学习，  
那一个具体的例子来印证zk真是一个非常不错的决定，虽然本屌很菜，坐标来自杭州某重本。闲话不说，  
我们来解析下源码  

#### 如何入手

tbschedule源码提供了一test文件，这个文件可以提供我们一定的指示。事实上，如果环境是争取的，这个test是可以跑起来的    


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

代码有点短，整个流程也就是这样的，等待资源初始化以及清理，然后向zk写入相应的ScheduleTaskType和SceduleStrategy。由zk负责分布式功能  
关注点在**scheduleManagerFactory**上面，这个是一个bean在schedule.xml中配置，配置如下:

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
作者都自己加了注释了，好明显啊，   
就是数据的初始化了。把正常的东西全都初始化话好，然后注册调度管理器。这里我们直接考虑的是调度，不是系统  

>首先先看zkManager.initial()里面做的是当然是在zk上创建相应的znode(层级比较哎)了，然后根据node里面的信息校验下，是否可用。  这里需要强调的是整个tbSchedule就是基于zk(依赖)  
>接下来看一下ScheduleDataManager4ZK(this.zkManager),这个也比较简单,继续创建znode层级，层级加深    
>然后ScheduleStrategyDataManager4ZK(this.zkManager)，同理  
----

	现在看一下目前的znode的层级  

	--/rootPath  
		---/facotry  
			---...省略  
		---/strategy  
			---...省略  
		---/baseTaskType  
			---...省略  
	省略标示目前没有  


#### 回归正题

来看一下Factroy的注册，对于一个java人员来说，factory一般的重要性不用多说了吧  

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

前面一段，很多但是很多简单，一句话创建znode层级,为了能在分布式上区分这些，作者做在路径上花费了心思，不过一般我们都能想的到。
区分分布式路径 **本机ip$本机名称$大写去横杆的UUID$zk生成的seq**  
tip:这些是临时节点哦

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

也很简单。功能无非是从zk得到相应的ScheduleStrategy  
值得说明的是这些**ScheduleStrategy**在上面的test文件，  
是通过创建写入zk形成的，所以这里我们可以从这边读取  
ScheduleStrategy的名称作为路径。其他信息序列化存在该路径作为数据    
所以我们看到了**this.loadStrategy(name)**

----

继续看，由于ScheduleStrategy里面保存相关分配机器的东西所以要，进行处理，如果是本机器，那么就可以跳过了，对于其他机器则是，或者本机的IP换过了  
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

逻辑很简单吧,5次执行，就会refresh下（前提是网络良好，因为上面也说了），5次也就是10s中，网上有一份京东高级工程师写的  
我也就呵呵了，本屌还没本科毕业呢。看重点:


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

代码依旧不是很长，我们稍微详细来介绍下。就是设置相应的属性，然后向znode某个路径注册了。 主要关注我写的注释关于zk下面的代码 
注册当前currenScheduleServer.  
然后znode层级发生了变化 现在的层级是这样的  


	
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
tip:一个进程只有一个Factory和Server偶  

回到程序继续看接下是一个心跳监测，先略过
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

代码稍微有点长，功能就是开启一个线程，仔细来看一下吧 关注点为**initialRunningInfo();**进去看一下吧 


	public void initialRunningInfo() throws Exception{
		scheduleCenter.clearExpireScheduleServer(this.currenScheduleServer.getTaskType(),this.taskTypeInfo.getJudgeDeadInterval());
		List<String> list = scheduleCenter.loadScheduleServerNames(this.currenScheduleServer.getTaskType());
		if(scheduleCenter.isLeader(this.currenScheduleServer.getUuid(),list)){
	    	//是第一次启动，先清楚所有的垃圾数据
			log.debug(this.currenScheduleServer.getUuid() + ":" + list.size());
	    	this.scheduleCenter.initialRunningInfo4Static(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getOwnSign(),this.currenScheduleServer.getUuid());
	    }
	 }
清楚功能就不看了，由leader来负责，意味着分布式中其他节点都在等这个操作  看一下这个操作  

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
							---/cur_server
							---/req_server
							---/sts(状态:active,finish,halt)
							---/parameter(任务处理需要的参数)
							---/deal_desc(任务处理情况,用于任务处理器会写一些信息)
						---/序号2
						---/序号3
						---...
				---/baseTaskTypeName1$runtime2
				---/baseTaskTypeName1$runtime3
				---...
			---/baseTaskTypeName2
			---/baseTaskTypeName3
			---... 
补充完毕，我们来说一下，就是初始化运行信息，有leader来初始化，这里的leader指的是ScheduleServer。选举算法也是seq最小  

初始化完毕后尝试调度队列的获取，一直尝试得到当前的调度队列。如何获得的，这时候心跳线程就是有用处了，进入心跳线程看一下吧 

	//心跳线程
	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			manager.refreshScheduleServerInfo();
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}
	}
继续观看

	/**
	 * 定时向数据配置中心更新当前服务器的心跳信息。
	 * 如果发现本次更新的时间如果已经超过了，服务器死亡的心跳周期，则不能在向服务器更新信息。
	 * 而应该当作新的服务器，进行重新注册。
	 * @throws Exception 
	 */
	public void refreshScheduleServerInfo() throws Exception {
	  try{
		rewriteScheduleInfo();
		//如果任务信息没有初始化成功，不做任务相关的处理
		//等待主leader初始化成功
		if(this.isRuntimeInfoInitial == false){
			return;
		}
		
        //重新分配任务(leader分配)
        this.assignScheduleTask();
        
        //判断是否需要重新加载任务队列，避免任务处理进程不必要的检查和等待
        boolean tmpBoolean = this.isNeedReLoadTaskItemList();
        if(tmpBoolean != this.isNeedReloadTaskItem){
        	//只要不相同，就设置需要重新装载，因为在心跳异常的时候，做了清理队列的事情，恢复后需要重新装载。
        	synchronized (NeedReloadTaskItemLock) {
        		this.isNeedReloadTaskItem = true;
        	}
        	rewriteScheduleInfo();
        }
        
        if(this.isPauseSchedule  == true || this.processor != null && processor.isSleeping() == true){
            //如果服务已经暂停了，则需要重新定时更新 cur_server 和 req_server
            //如果服务没有暂停，一定不能调用的
               this.getCurrentScheduleTaskItemListNow();
          }
		}catch(Throwable e){
			//清除内存中所有的已经取得的数据和任务队列,避免心跳线程失败时候导致的数据重复
			this.clearMemoInfo();
			if(e instanceof Exception){
				throw (Exception)e;
			}else{
			   throw new Exception(e.getMessage(),e);
			}
		}
	}	
注释很明白，每次心跳向zk写入信息，等待leader初始化话完毕，初始化成功后。进行分配任务   

	/**
	 * 根据当前调度服务器的信息，重新计算分配所有的调度任务
	 * 任务的分配是需要加锁，避免数据分配错误。为了避免数据锁带来的负面作用，通过版本号来达到锁的目的
	 * 
	 * 1、获取任务状态的版本号
	 * 2、获取所有的服务器注册信息和任务队列信息
	 * 3、清除已经超过心跳周期的服务器注册信息
	 * 3、重新计算任务分配
	 * 4、更新任务状态的版本号【乐观锁】
	 * 5、根系任务队列的分配信息
	 * @throws Exception 
	 */
	public void assignScheduleTask() throws Exception {
		scheduleCenter.clearExpireScheduleServer(this.currenScheduleServer.getTaskType(),this.taskTypeInfo.getJudgeDeadInterval());
		List<String> serverList = scheduleCenter
				.loadScheduleServerNames(this.currenScheduleServer.getTaskType());
		
		if(scheduleCenter.isLeader(this.currenScheduleServer.getUuid(), serverList)==false){
			if(log.isDebugEnabled()){
			   log.debug(this.currenScheduleServer.getUuid() +":不是负责任务分配的Leader,直接返回");
			}
			return;
		}
		//设置初始化成功标准，避免在leader转换的时候，新增的线程组初始化失败
		scheduleCenter.setInitialRunningInfoSucuss(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getTaskType(), this.currenScheduleServer.getUuid());
		scheduleCenter.clearTaskItem(this.currenScheduleServer.getTaskType(), serverList);
		scheduleCenter.assignTaskItem(this.currenScheduleServer.getTaskType(),this.currenScheduleServer.getUuid(),this.taskTypeInfo.getMaxTaskItemsOfOneThreadGroup(),serverList);
	}
这个也是leader来执行的。如果是leader立马向zk写入相关serverUUID，然后清除相应的TaskItem，也就是在node/cur_server下，
然后分配TaskItem,在node上写入相应的数据，装载完毕设置node/server下的数据(reload=true).就是加载当前的任务队列，这里通过乐观锁来实现的额  
这里分配后，尝试获取队列就不会被阻塞了，就是继续往下执行。
加载任务的总量。最后执行computerStart()  

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

代码看起来有点乱，但是思路还是明显的。通过定时器来执行。
最后this.resume("任务开始执行")

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

