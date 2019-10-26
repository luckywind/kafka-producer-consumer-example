###why SimpleConsumer
1,多次消费
2，只消费特定分区的消息
3，管理事务确保一条消息仅处理一次
###缺点
You must keep track of the offsets in your application to know where you left off consuming.
You must figure out which Broker is the lead Broker for a topic and partition
You must handle Broker leader changes
###Steps for using a SimpleConsumer
找leader 
Find an active Broker and find out which Broker is the leader for your topic and partition
找replica
Determine who the replica Brokers are for your topic and partition
构造数据获取请求
Build the request defining what data you are interested in
获取数据
Fetch the data
识别并从leader变更中恢复
Identify and recover from leader changes

