# mooc_weblog
2018-11-30，Spark SQL分析mooc日志

# 1 功能实现
* 使用 Spark SQL 解析访问日志
* 解析出课程编号、类型
* 根据 ip 解析出城市信息
* 使用 Spark SQL 将访问时间按照天进行分区输出

# 2 时间格式
* 输入 ：访问时间、访问URL、流量、访问IP
* 输出 ：URL,cmsType(video/article),cmsId(编号),流量，ip,城市,访问时间,天
