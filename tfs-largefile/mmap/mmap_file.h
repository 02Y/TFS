#ifndef _MMAP_FILE_H_      //large file mmap
#define _MMAP_FILE_H_

#include "common.h"

namespace conway
{
	namespace largefile
	{
		class MMapFile
		{
			public:
			  MMapFile();
			 
			  explicit MMapFile(const int fd);
			  MMapFile(const MMapOption& mmap_option, const int fd);
			 
			  ~MMapFile();
			 
			  bool sync_file();    //同步内存数据到文件
			  bool map_file(const bool write = false);  //将文件映射到内存，同时设置访问权限
			  void* get_data() const;     //获取映射到内存数据的首地址
			  int32_t get_size() const;   //获取映射数据的大小
			 
			  bool munmap_file();   //解除映射
			  bool mremap_file();    //重新执行映射
			 
			private:
			  bool ensure_file_size(const int32_t size);  //扩容
			  
			private:
			  int32_t size_;
			  int fd_;
			  void* data_;				  
			  struct MMapOption mmap_file_option_;
		};
	}
}






#endif   //_MMAP_FILE_H_