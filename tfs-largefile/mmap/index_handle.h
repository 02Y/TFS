#ifndef _INDEX_HANDLE_H_
#define _INDEX_HANDLE_H_

#include "common.h"
#include "mmap_file_op.h"

using namespace std;

namespace conway
{
	namespace largefile
	{
		struct IndexHeader
		{
		  public:
			IndexHeader()
			{
				memset(this, 0, sizeof(IndexHeader));
			}
			
			BlockInfo block_info;        //meta block info
			int32_t bucket_size_;        //hash bucket size(个数)
			int32_t data_file_offset_;   //offset to write next data in block
			int32_t index_file_size_;    //offset after index_header + all buckets
			int32_t free_head_offset_;    //free meta node list, for reuses		
		};
		
		class IndexHandle
		{
		  public:
			IndexHandle(const string& base_path, const uint32_t main_block_id);    //create block index
			~IndexHandle();
			
			int create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option);      //create index file and mmap index file to RAM
			int load(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option);        //(exist and not mmap)mmap index file to RAM
			
			int remove(const uint32_t logic_block_id);     //remove index：munmap and unlink file
			
			int flush();
			
			int update_block_info(const OperType oper_type, const uint32_t modify_size);
			
			IndexHeader* index_header()      //if TFS_SUCCESS return file addr.
			{
				return reinterpret_cast<IndexHeader*>(file_op_->get_map_data());
			} 
			
			BlockInfo* block_info()
			{
				return reinterpret_cast<BlockInfo*>(file_op_->get_map_data());
			}
			
			int32_t* bucket_slot()
			{
				return reinterpret_cast<int32_t*>(reinterpret_cast<char*>(file_op_->get_map_data()) + sizeof(IndexHeader));
			}
			
			int32_t bct_size() const
			{
				return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->bucket_size_;
			}
			
			int32_t get_block_data_offset() const
			{
				return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->data_file_offset_;
			}
			
			int32_t free_head_offset() const
			{
				return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->free_head_offset_;
			}
			
			void commit_block_data_offset(const int file_size)
			{
				reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->data_file_offset_ += file_size;
			}
			
			int32_t write_segment_meta(const uint64_t key, MetaInfo& meta);       //写入metainfo
			int32_t read_segment_meta(const uint64_t key, MetaInfo& meta);        //读出metainfo
			int32_t delete_segment_meta(const uint64_t key);
			
			int32_t hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset);
			
			int32_t hash_insert(const uint64_t key, int32_t previous_offset, MetaInfo& meta);
			
			int32_t block_tidy(FileOperation* fo);                //整理块
			
		  private:
			bool hash_compare(const uint64_t left_key, const uint64_t right_key)
			{
				return (left_key == right_key);
			}
		  
			MMapFileOperation* file_op_;    
			bool is_load_;       //是否已经加载
		};
	}
}

#endif     //_INDEX_HANDLE_H_