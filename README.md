# TFS
tfs-largefile     淘宝文件系统，核心存储引擎（Linux）

需处理优化项：
      清理块函数(block_tidy)需处理bug 进行优化，设计接口按时调用
          ___bug描述：如果读写失败会造成数据错乱丢失
