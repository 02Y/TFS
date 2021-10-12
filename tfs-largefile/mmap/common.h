#ifndef _COMMON_H_
#define _COMMON_H_

#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include <iostream>
#include <string>
#include <fcntl.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <assert.h>

namespace conway
{
	namespace largefile
	{
		const int32_t TFS_SUCCESS = 0;
		const int32_t TFS_ERROR = -1;       
		const int32_t EXIT_DISK_OPER_INCOMPLETE = -8012;         //read or write length is less than required.
		const int32_t EXIT_INDEX_ALREADY_LOADED_ERROR = -8013;   //index is loaded when create or load.
		const int32_t EXIT_META_UNEXPECT_FOUND_ERROR = -8014;    //meta found in index when insert.
		const int32_t EXIT_INDEX_CORRUPT_ERROR = -8015;	         //index is corrupt.
		const int32_t EXIT_BLOCKID_CONFLICT_ERROR = -8016;       //block id conflict.
		const int32_t EXIT_BUCKET_CONFIGURE_ERROR = -8017;       //old bucket != new bucket.
		const int32_t EXIT_META_NOT_FOUND_ERROR = -8018;         //not found key(状态)
		const int32_t EXIT_BLOCKID_ZERO_ERROR = -8019;           //block id == 0
		const int32_t EXIT_BLOCK_NOT_EXIST = -8020;              //block file not exist
		const int32_t EXIT_BLOCK_DEL_FILE_COUNT_LESSZERO = -8021;    //block del_file_count_ <= 0
		const int32_t EXIT_BLOCK_DEL_SIZE_LESSZEOR= -8022;       //block del_size_ <=0
		
		static const std::string MAINBLOCK_DIR_PREFIX = "/mainblock/";   //linux下是“/”
		static const std::string INDEX_DIR_PREFIX = "/index/";
		static const mode_t DIR_MODE = 0755;
		
		enum OperType
		{
			C_OPER_INSERT = 1,
			C_OPER_DELETE
		};
		
		struct MMapOption
		{
			int32_t max_mmap_size_;        //最大大小
			int32_t first_mmap_size_;      //初始大小
			int32_t per_mmap_size_;        //扩容大小
		};
		
		struct BlockInfo       //块索引信息
		{
			uint32_t block_id_;        //块编号
			int32_t version_;          //块当前版本号(只要进行改动就变动版本号)
			int32_t file_count_;       //当前已保存文件总数
			int32_t size_t_;           //当前已保存文件数据总大小
			int32_t del_file_count_;   //已删除的文件数量
			int32_t del_size_;         //已删除的文件数据总大小
			uint32_t seq_no_;          //下一个可分配的文件编号
			
			BlockInfo()       //构造函数
			{
				memset(this, 0, sizeof(BlockInfo));       //所有属性全部清零
			}
		
			inline bool operator==(const BlockInfo& rhs) const
			{
				return block_id_ == rhs.block_id_ && version_ == rhs.version_ && file_count_ == rhs.file_count_
						&& size_t_ == rhs.size_t_ && del_file_count_ == rhs.del_file_count_ && del_size_ == rhs.del_size_
						&& seq_no_ == rhs.seq_no_;
			}
		};
		
		struct MetaInfo    //小文件索引信息 
		{
		  public:
		    MetaInfo()    //自定义默认构造
			{
				init();
			}
			
			MetaInfo(const uint64_t file_id, const int32_t in_offset, 
				const int32_t file_size, const int32_t next_meta_offset_)   //带参构造
			{
				fileid_ = file_id;
				location_.inner_offset_ = in_offset;
				location_.size_ = file_size;
				this->next_meta_offset_ = next_meta_offset_;	
			}
			
			MetaInfo(const MetaInfo& meta_info)    //拷贝构造
			{
				memcpy(this, &meta_info, sizeof(MetaInfo));
			}
			
			MetaInfo& operator=(const MetaInfo& meta_info)   //重载复制构造
			{
				if(this == &meta_info)    
				{
					return *this;
				}
				fileid_ = meta_info.fileid_;
				location_.inner_offset_ = meta_info.location_.inner_offset_;
				location_.size_ = meta_info.location_.size_;
				next_meta_offset_ = meta_info.next_meta_offset_;				
			}
			
			MetaInfo& clone(const MetaInfo& meta_info)       //克隆
			{
				assert(this != &meta_info);
				
				fileid_ = meta_info.fileid_;
				location_.inner_offset_ = meta_info.location_.inner_offset_;
				location_.size_ = meta_info.location_.size_;
				next_meta_offset_ = meta_info.next_meta_offset_;
				
				return *this;
			}
			
			bool operator==(const MetaInfo& rhs) const       //重载==运算符
			{
				return fileid_ == rhs.fileid_ && location_.inner_offset_ == rhs.location_.inner_offset_ &&
					location_.size_ == rhs.location_.size_ && next_meta_offset_ == rhs.next_meta_offset_;
			}
			
			uint64_t get_key() const
			{
				return fileid_;
			}
			
			void set_key(const uint64_t key)
			{
				fileid_ = key;
			}
			
			uint64_t get_file_id() const
			{
				return fileid_;
			}
			
			void set_file_id(const uint64_t file_id)
			{
				fileid_ = file_id;
			}
			
			int32_t get_offset() const
			{
				return location_.inner_offset_;
			}
			
			void set_offset(const int32_t offset)
			{
				location_.inner_offset_ = offset;
			}
			
			int32_t get_size() const
			{
				return location_.size_;
			}
			
			void set_size(const int32_t file_size)
			{
				location_.size_ = file_size;
			}
			
			int32_t get_next_meta_offset() const
			{
				return next_meta_offset_;
			}
			
			void set_next_meta_offset(const int32_t offset)
			{
				next_meta_offset_ = offset;
			}
			
			
		  private:
			uint64_t fileid_;   //文件编号
			
			struct {
				int32_t inner_offset_;    //文件在块内部的偏移量
				int32_t size_;            //文件大小
			}location_;
			
			int32_t next_meta_offset_;    //当前哈希链下一个节点在索引文件中的偏移量
			
		  private:
			void init()
			{
				fileid_ = 0;
				location_.inner_offset_ = 0;
				location_.size_ = 0;
				next_meta_offset_ = 0;
			}
		};
		
	}
}

#endif  //_COMMON_H_