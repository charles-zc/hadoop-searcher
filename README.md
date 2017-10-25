# hadoop-searcher
基于Lucene和HDFS的PB级数据索引、搜索、存储系统

 •应用数据量大而ES集群管理成本过高且用户需求又不多的场景；
 •优点节约成本并且Hadoop相对稳定，缺点量大速度相对不快（后面尝试用spark做）。
   数据存储在ES中，关掉所有2个月前的索引；
   把包含历史数据的index存储在HDFS中；
   抛弃ES直接调用Lucene接口查询字段；
   对查询过程做MapReduce，返回查询结果。
