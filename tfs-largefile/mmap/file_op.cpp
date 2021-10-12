#include "file_op.h"

namespace conway
{
	namespace largefile
	{
		FileOperation::FileOperation(const std::string& file_name, const int open_flags):
		fd_(-1), open_flags_(open_flags)
		{
			file_name_ = strdup(file_name.c_str());   //复制字符串  //free
		}
	
		FileOperation::~FileOperation()
		{
			if(fd_ > 0)
			{
				::close(fd_);
			}
			
			if(NULL != file_name_)
			{
				free(file_name_);
				file_name_ = NULL;
			}
		}
		
		int FileOperation::open_file()
		{
			if(fd_ > 0)
			{
				::close(fd_);
				fd_ = -1;
			}
			
			fd_ = ::open(file_name_, open_flags_, OPEN_MODE);
			
			if(fd_ < 0) return -errno;
			
			return fd_;
		}
		
		void FileOperation::close_file()
		{
			if(fd_ < 0) return;
			
			::close(fd_);
			fd_ = -1;
		}
		
		int FileOperation::pread_file(char* buf, const int32_t nbytes, const int64_t offset)
		{
			int32_t left = nbytes;          //剩余的字节数
			int64_t read_offset = offset;   //起始位置的偏移量
			int32_t read_len = 0;           //已读的长度
			char* p_tmp = buf;              //已读取的缓冲的位置
			
			int i = 0;
			
			while(left > 0)
			{
				++i;
				
				if(i >= MAX_DISK_TIMES)   //超过读取次数
				{
					break;
				}
				
				if(check_file() < 0)      //检查文件状态
				{
					return -errno;        //打开失败
				}
				
				read_len = ::pread64(fd_, p_tmp, left, read_offset);
				
				if(read_len < 0)      //读取失败
				{
					read_len = -errno;
					
					if(-read_len == EINTR || EAGAIN == -read_len)
					{
						continue;
					}
					else if(EBADF == -read_len)
					{
						fd_ = -1;
						continue;
					}
					else
					{
						return read_len;
					}
				}
				else if(0 == read_len)   //读到文件尾
				{
					break;
				}
				                          /*读取成功*/
				left -= read_len;         //重置剩余字节数
				p_tmp += read_len;        //重置读取缓冲的位置
				read_offset += read_len;  //重置读取偏移量
			}
			if(0 != left)
			{
				return EXIT_DISK_OPER_INCOMPLETE;   //未达到预期（文件未读全）
			}
			
			
			return TFS_SUCCESS;      //文件读取成功
		}
		
		int FileOperation::pwrite_file(const char* buf, const int32_t nbytes, const int64_t offset)
		{
			int32_t left = nbytes;          //需要写入的剩余的字节数
			int64_t write_offset = offset;  //起始位置的偏移量
			int32_t written_len = 0;        //已写的长度
			const char* p_tmp = buf;              //已写到的缓冲的位置
			
			int i = 0;
			
			while(left > 0)
			{
				++i;
				
				if(i >= MAX_DISK_TIMES)   //超过写入次数
				{
					break;
				}
				
				if(check_file() < 0)      //检查文件状态
				{
					return -errno;        //打开失败
				}
				
				written_len = ::pwrite64(fd_, p_tmp, left, write_offset);
				
				if(written_len < 0)      //写入失败
				{
					written_len = -errno;
					
					if(-written_len == EINTR || EAGAIN == -written_len)
					{
						continue;
					}
					else if(EBADF == -written_len)
					{
						fd_ = -1;
						continue;
					}
					else
					{
						return written_len;
					}
				}
				                              /*读取成功*/
				left -= written_len;          //重置剩余字节数
				p_tmp += written_len;         //重置已写入的缓冲的位置
				write_offset += written_len;  //重置写入偏移量
			}
			if(0 != left)
			{
				return EXIT_DISK_OPER_INCOMPLETE;   //未达到预期（文件未写全）
			}
			
			
			return TFS_SUCCESS;      //文件写入成功
			
		}
		
		int FileOperation::write_file(const char* buf, const int32_t nbytes)
		{
			int32_t left = nbytes;          //需要写入的剩余的字节数
			//int64_t write_offset = 0;       //起始位置的偏移量
			int32_t written_len = 0;        //已写的长度
			const char* p_tmp = buf;              //已写到的缓冲的位置
			
			int i = 0;
			
			while(left > 0)
			{
				++i;
				
				if(i > MAX_DISK_TIMES)   //超过写入次数
				{
					break;      
				}
				
				if(check_file() < 0)     //检查文件状态
				{
					return -errno;       //打开失败
				}
				
				//written_len = ::pwrite(fd_, p_tmp, left, write_offset);
				written_len = ::write(fd_, p_tmp, left);
				
				if(written_len < 0)      //写入失败
				{
					written_len = -errno;
					
					if(-written_len == EINTR || -written_len == EAGAIN)
					{
						continue;
					}
					else if(-written_len == EBADF)
					{
						fd_ = -1;
						continue;
					}
					else
					{
						return written_len;
					}
				}
										          /*写入成功*/
				left -= written_len;              //重置剩余字节数				
				p_tmp += written_len;             //重置已写入的缓冲的位置
				//write_offset += written_len;      //重置写入偏移量
			}
			if(0 != left)
			{
				return EXIT_DISK_OPER_INCOMPLETE;       //未达到预期（文件未写全）
			}
			
			return TFS_SUCCESS;
		}
		
		int64_t FileOperation::get_file_size()
		{
			int fd = check_file();
			
			if(fd < 0)
			{
				return -1;
			}
			struct stat stbuf;
			if(fstat(fd_, &stbuf) != 0)
			{
				return -1;
			}
			
			return stbuf.st_size;
		}
		
		int FileOperation::check_file()
		{
			if(fd_ < 0)
			{
				fd_ = open_file();
			}
			
			return fd_;
		}
		
		int FileOperation::ftruncate_file(const int64_t length)
		{
			int fd = check_file();
			
			if(fd < 0)
			{
				return fd;
			}
			
			return ftruncate(fd, length);
		}
		
		int FileOperation::seek_file(const int64_t offset)
		{
			int fd = check_file();
			
			if(fd < 0)
			{
				return fd;
			}
			
			return lseek(fd, offset, SEEK_SET); //offset即为新的读写位置
		}
		
		int FileOperation::flush_file()
		{
			if(open_flags_ & O_SYNC)
			{
				return 0;
			}
			
			int fd = check_file();
			if(fd < 0)
			{
				return fd;
			}
			
			return fsync(fd);
		}
		
		int FileOperation::unlink_file()
		{
			close_file();
			return unlink(file_name_);
		}
		
/*		int32_t block_tidy(IndexHandle* index_handle)
		{
			
			//查看del_file_count
			//根据文件编号，逐步从头部开始写， hash_find 没有就continue，有就往前写
			//截断文件
			//更新索引文件信息
			
			if(!index_handle)          //索引文件不存在
			{
				return EXIT_INDEX_NOT_EXIST;
			}
			
			if(index_handle->block_info().del_file_count_ <= 0)        //块删除文件数量小于0
			{
				fprintf(stderr, "block id %u do not have del_file. del_file_count：%d\n", index_handle->block_info().block_id_, index_handle->block_info().del_file_count_);
				return EXIT_BLOCK_DEL_FILE_COUNT_LESSZERO;
			}
			
			if(index_handle->block_info().del_size_ <= 0)             //块删除文件大小小于0
			{
				fprintf(stderr, "block id %u do not have del_file_size. del_file_size：%d\n", index_handle->block_info().block_id_, index_handle->block_info().del_size_);
				return EXIT_BLOCK_DEL_SIZE_LESSZEOR;
			}
			
			int32_t file_count = index_handle->block_info().file_count_;        //文件数量
			int32_t ret = TFS_SUCCESS;
			int32_t over_write_offset = 0;       //整个文件写入块后的偏移量
			int32_t current_write_offset = 0;    //文件未写全，块中的偏移量
			int nbytes = sizeof(buffer);        //该次需要写入的字节数
			int residue_bytes = 0;        		//写入后还剩下需要写的字节数

			uint64_t key = 1;             //保存文件编号
			char* buffer = new char [4096];      //保存的文件
			
			//整理块
			for(int i = 1; i <= file_count; )
			{
				MetaInfo meta_info;            //保存临时读到的metainfo
				ret = index_handle->read_segment_meta(key, meta_info);
				
				current_write_offset = meta_info.get_offset();   
				residue_bytes = meta_info.get_size(); 
			
				if(TFS_SUCCESS == ret)           //已经在哈希链表中读到
				{
					if(meta_info.get_size() < sizeof(buffer))        //一次读完
					{
						ret = pread_file(buffer, meta_info.get_size(), meta_info.get_offset());      
						if(ret == TFS_SUCCESS)    //文件读成功,将文件重新写入块中
						{
							ret = pwrite_file(buffer, meta_info.get_size(), over_write_offset);
							if(ret == TFS_SUCCESS)          //文件写入成功
							{
								over_write_offset += meta_info.get_size();
							}
							else         //文件未写成功 / 未写全
							{
								return ret;           //可以考虑将读取文件的地址传回(buffer的地址)
							}
						}
						else           //文件未读成功 / 未读全
						{
							return ret;
						}
					}
					else         //需要分多次读写
					{
						for(int j = 0; j < 1; )
						{
							ret = pread_file(buffer, nbytes, current_write_offset);     
							if(ret == TFS_SUCCESS)    		//文件读成功,将部分文件重新写入块中
							{
								ret = pwrite_file(buffer, nbytes, over_write_offset);
								if(ret == TFS_SUCCESS)          //文件写入成功
								{
									current_write_offset += nbytes;
									over_write_offset += nbytes;
									residue_bytes -= nbytes;
									if(nbytes > residue_bytes)
									{
										if(0 == residue_bytes) j++;        //结束循环
										
										nbytes = residue_bytes;
										continue;
									}
									else
									{
										continue;
									}
									
								}
								else         //文件未写成功 / 未写全
								{
									return ret;           //可以考虑将读取文件的地址传回(buffer的地址)
								}
							}
							else
							{
								return ret;   //文件未读成功 / 未读全
							}
						}
					}
					
				}
				else if(EXIT_META_NOT_FOUND_ERROR != ret)     //not found key(状态)
				{
					return ret;
				}
				else if(EXIT_META_NOT_FOUND_ERROR == ret)     //哈希链表中没有找到,该文件已被删除
				{
					key++;
					continue;
				}
			
				i++;
				key++;
			}
			
			ret = flush_file();
			//截断文件
			ret = ftruncate_file(index_handle->block_info().size_t_);
			
			//更新block info
			ret = index_handle->block_info().del_file_count_ = 0;
			ret = index_handle->block_info().del_size_ = 0;
			index_handle->flush();
			
			return TFS_SUCCESS;
		}
*/		
	}
}