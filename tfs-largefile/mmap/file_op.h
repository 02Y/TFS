#ifndef _FILE_OP_H_     //large file operation
#define _FILE_OP_H_

#include "common.h"


namespace conway
{
	namespace largefile
	{
		class FileOperation
		{
			public:
			  FileOperation(const std::string& file_name, 
				const int open_flags = O_RDWR|O_LARGEFILE);
			  ~FileOperation();
	
			  int open_file();   //打开文件
			  void close_file(); //关闭文件
			
			  int flush_file();  //把文件立即写入到磁盘			
			  int unlink_file(); //删除文件
			  
			  virtual int pread_file(char* buf, const int32_t nbytes, const int64_t offset);  //读 带偏移量
			  virtual int pwrite_file(const char* buf, const int32_t nbytes, const int64_t offset);	//写 带偏移量  
			  int write_file(const char* buf, const int32_t nbytes);  //直接写
			  
			  int64_t get_file_size();   //获取文件大小
			  
			  int ftruncate_file(const int64_t length);  //截断文件
			  int seek_file(const int64_t offset);   //从文件中进行移动
			  
			  int get_fd() const { return fd_; }   //获取文件句柄
			  
			  //int32_t block_tidy(IndexHandle& index_handle);                //整理块
			  
			protected:
			  int check_file();   //检查文件状态（能否打开）
			  
			protected:
			  int fd_;
			  int open_flags_;    //打开文件旗标
			  char* file_name_;   //文件名
			  
			protected:
			  static const mode_t OPEN_MODE = 0644;   //权限 0UGO
			  static const int MAX_DISK_TIMES = 5;    //最大的磁盘读取次数
		};
	}
}



#endif  //_FILE_OP_H_