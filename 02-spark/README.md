参考：[spark-streaming-basis](https://github.com/heibaiying/BigData-Notes/tree/master/code/spark/spark-streaming-basis)
编译错误1：java: 错误: 不支持发行版本 5
修改 【设置】-【构建、执行、部署】-【编译器】-【Java编译器】选择9版本
编译错误2：illegal cyclic inheritance involving trait Iterable
Scala版本问题，改为2.12.8