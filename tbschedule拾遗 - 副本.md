## tbschedule简介
---

#### 序言

tbschedule是一个淘宝早期的分布式任务调度框架，其功能特性实现依赖于zookeeper。  

#### tbschedule启动流程

* web形式的tbschedule借助一个HttpServlet子类来在web容器启动后进行初始化
* **ConsoleManager**这个工具类提供了我们简单的初始化方法，适用于jar，war等形式
* 配置tbschedule的相关属性的两种配置方法。  
    1.  配置文件的形式，采用这种的tbschedule应用通常来展现一个控制界面。参与控制 
    2.  spring标准配置，采用这种方式的应用将参与任务调度
* **TBScheduleManagerFatory**类是一个配置工厂也是整个分布式调度任务框架的核心。

#### tbschedule的应用流程

* 入口点**TBScheduleManagerFatory**的**init(Proprties p)**上无论是参与控制的tb应用还是task应用  
    1. 停掉初始化化线程，这个线程保持了对zk的尝试连接，这个简单停掉也只是让线程连不上zk时及早返回。
    2. 接下来是一个加锁的过程，进行了各种关键对象的置空，资源释放，以及向外提供一个有效的工具类**ConsoleManager** 
    3. 这里有三个对象，数据管理对象，策略管理对象和一个zk管理对象。前两个完全依赖zk管理对象
    4. 然后讲首先初始化化zk管理对象，因为zk连不上，应用也没有起来的意义
    5. 之后委托给初始化线程进行初始化。
* 关键对象的初始化**TBScheduleManagerFatory**的**initalData()**，比较上面方法，能觉得上面就是为了连上zk  
    1. 设置zk上的根znode的信息，显然是zk管理对象的任务 
    2. 初始化数据管理对象,策略管理对象。如同上面所说，这两个依赖于zk管理对象
    3. 判断应用是那种形式的应用，参与任务，参与控制？后者到此为止了，因为足够了。

#### 数据管理对象和策略管理对象的注册 

> 数据管理对象和策略管理对象都是该任务调度框架中的重要对象。无论是那种形式的应用都需要这两个。

##### new ScheduleDataManager4ZK(this.zkManager);
* 构造这样一个关于数据的zNode路径

        XXXX

##### new ScheduleStrategyDataManager4ZK(this.zkManager);
* 构造这样一个关于策略的zNode路径

        XXXX
-----

#### tbschedule的任务调度 

> 上面说到如果是参与控制的应用，就完了，但是绝大数应用都是参与任务调度，因此还要继续初始化。  

##### 调度过程
* **TBScheduleManagerFatory**的属性具有机器信息，因此他会被注册到zk上,具体是策略管理对象
同理因为无法知道zk上是否有跟该对象的垃圾信息，所以要做点逻辑进行清理    
* 2s一次机器相关信息刷新，会检测与zk的连接状态，10s没连上，清理所有数据任务，重启服务，任务的重启是这样的
    1. 为开始的任务，之间清除
    2. 以开始的任务，该次做完后清除
* 2s一次信息刷新由**TBScheduleManagerFatory**方法refresh封装


#### tbschedule的路径图构建

>见思维导图

tip: zNode路径主要由任务和策略构成

#### 2S一次的refresh方法

* 获取机器信息(机器指的不同应用，而不是物理机器)
* 清理工作
* 重新注册信息

#### reRegisterManagerFactory
* 这一步做的是上面调度过程中的第一点，**TBScheduleManagerFatory**的属性具有机器信息，因此他会被注册到zk上.  
因为信息发生了改变，所以要进行逻辑的清理
* 对于不属于分配给本台机器任务的，进行相关任务的结束，当然这个结束实在该次任务跑完之后，如果任务在运行的话 
* 根据策略重新分配调度任务的机器。这个是由leader来做的，但是每台机器都会去尝试

#### leader分配资源
* leader的产生方式，zk的master选举,seq最小为leader 
* 分配策略来自写入zNode节点的对象序列化信息(ScheduleStrategy.class)
* 策略的分配方式-----一个工具方法**assignTaskNumber**取模平均分配，源码的这里我们可以进行定制分配方式。
* 更新机器上的运行时策略信息(ScheduleStrategyRunntime.class)


#### 根据运行时策略信息，增加或者减少调度服务(IStrategyTask)
* 调度服务是一个列表，总是被缓存在内存中
* 减少总是简单移除列表的末尾，然后尝试去停止服务，这个尝试也就是会等待运行中的任务跑完
* 增加，在目前的tbschedule中总是一样的去创建**TBScheduleManagerStatic**


#### 真正的服务处理器TBScheduleManagerStatic
>这是一个拥有所有信息的服务处理器
1. 机器对象
2. 数据管理对象
3. 任务信息对象(ScheduleTaskType.class)
4. 任务Bean
5. 当前的调度服务对象(ScheduleServer.class)

* TBScheduleManagerStatic总是拥有一个ScheduleServer。而且他们是一一映射的
* TBScheduleManagerStatic总是拥有一个心跳线程

#### TBScheduleManagerStatic的initial方法
>这是任务开始的地方
* 不同JVM中可能存在这样的TBScheduleManagerStatic为一个策略任务运行，
因此这里也有这样的方式等待leader发号施令
* 申请任务项队列
* 开始执行

#### leader发号施令之分配任务
>分配任务在心跳线程中，心跳线程定时刷新信息
* 先做清理工作
* 尽快的设置初始化话成功标志，leader标志
* 清理任务项
* 分配任务项

#### 分配任务项assignTaskItem
>跟上面的很相似
* 一旦有任务调整，要重新装载任务


#### computerStart()执行函数
* 根据配置执行任务，是否马上开启。什么时间开始执行
* timer,tasker执行

#### 最终执行函数resume()
>两种模式

1. SLEEP-----TBScheduleProcessorSleep

2. NOSLEEP-----TBScheduleProcessorNotSleep

##### NOTSLEEP模式
>TBScheduleProcessorNotSleep 会拥有所有的信息，
1. 开启线程进行跑任务
2. 加载数据。这里会判读是否加载过了
3. 休息一会
4. copy数据
5. 加载任务项定义
6. 执行bean的select 
7. 更新znode信息
8. 填充数据到taskList 
9. 判断是否有数据，根据配置是否sleep
10. 执行
##### SLEEP模式

* 类似

-----

### 拾遗

之前我们解析了源码还有很多没有解释的很明白，也没有指出在tbschedule在高可用和一致性
是如何通过zk来保证的。这一次我们带着这个需求来看整个流程,并在相关点中指出如何保障的。 

#### 情境模拟  

假设现在有一个tbschedule集群正在工作中。由于性能的问题，要对集群进行扩容。
我们假设是增加一个节点(tbschedule)。  
>首先做的是当然是改变策略,也就是ScheduleStragey这个中的IPList的属性，当然这个是不需要重启应用的。  
>还有一种简单的方式,每种方式都带上localhost那么，，任意节点加入都会执行该任务    


### **高可用**通过master选举


**master选举**

zk的特性，全局唯一序列，tbschedule利用这个seq

 **主节点挂了?**

 ---
 
 >这的确是我们考究的问题。高可用，一致性是如何保证。
 CAP理论中网络分区性是一定的，通常是在可用性和一致性的互妥协。
 那么到底如何保证现在的任务工作呢。zk本来就是高可用，和一致性的实现，当然这个一致性是基于base理论的  
 tbschedule依赖与zk，自然也得到了这些特性。

 
 >作者使用zk的时候，使用了临时节点，主节点挂了之后，等待会话超时后，zk就会自动移除主节点(临时节点），
 在下次刷新中，seq最小的从节点晋升为主节点。
 但是，主节点也是分配着任务的。并且根据作者的取模均分策略，主节点通常会多承担一点任务的。
 于是主节点死了，主节点的任务也就死掉了的，所以对任务的分配该是怎么样的呢。需要在代码上有一点的保证。
 
 >当session过期后，那么因为可以重新分配任务了那么。根据配置，以及网络的延迟的原因，当主节点挂掉之后，会用10s
 左右的主节点上的任务宕机掉，这是我们选择tbschedule时需要考虑的。

 >当然我们在客户端设置是session还要注意到zkserver设置的时间，这里设置不好也可能出现问题，但是是一个小问题需要稍微注意下

 >这样leader挂掉的故障转移就完成了

  **从节点挂了?**
 
 ---

 也是一个道理，从节点也是临时节点，会话过期后也会被摘除。  
 leader也不会向这个节点分配任务了，因为zk上已经找不到响应的信息了。  
 与上面所说的一致，你必须考虑在宕机的期间，你的任务的处理是否能容忍这种延时。

