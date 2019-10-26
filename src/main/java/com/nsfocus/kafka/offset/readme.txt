定制consumer，传入第三个参数手动控制offset
0：从头开始消费
-1:从尾开始消费，即只消费该consumser启动后的消息
其他值：只消费offset比指定offset大的消息。
http://blog.empeccableweb.com/wp/2016/11/30/manual-offsets-in-kafka-consumers-example/
