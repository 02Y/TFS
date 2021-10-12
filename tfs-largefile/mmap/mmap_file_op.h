#ifndef _MMAP_FILE_OP_H_
#define _MMAP_FILE_OP_H_

#include "common.h"
#include "file_op.h"
#include "mmap_file.h"

using namespace std;

namespace conway
{
	namespace largefile
	{
		class MMapFileOperation: public FileOperation
		{
			public:
			  MMapFileOperation(const string& file_name, 
				const int open_flags = O_RDWR | O_LARGEFILE | O_CREAT):
			    FileOperation(file_name, open_flags), map_file_(NULL), is_mapped_(false) {}
			  
			  
			  ~MMapFileOperation()
			  {
				  if(map_file_)
				  {
					  delete(map_file_);
					  map_file_ = NULL;
				  }
			  }
			  			  
			  int mmap_file(const MMapOption& mmap_option);
			  int munmap_file();
			  
			  int pread_file(char* buf, const int32_t size, const int64_t offset);
			  int pwrite_file(const char* buf, const int32_t size, const int64_t offset);
			  
			  void* get_map_data() const;   //获取文件地址
			  int flush_file();         //把文件立即写入到磁盘	
			  
			private:
			  MMapFile* map_file_;   //指向文件映射操作的对象
			  bool is_mapped_;       //文件是否已经映射
		};
	}
}
#endif     //_MMAP_FILE_OP_H_