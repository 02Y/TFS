#include "common.h"
#include "index_handle.h"
#include <sstream>

static int debug = 1;

using namespace std;

namespace conway
{
	namespace largefile
	{
		IndexHandle::IndexHandle(const string& base_path, const uint32_t main_block_id)
		{
			//creat file_op_ handle object
			stringstream tmp_stream;
			tmp_stream << base_path << INDEX_DIR_PREFIX  << main_block_id;  // /root/martin/index/1
			
			string index_path;
			tmp_stream >> index_path;
			
			file_op_ = new MMapFileOperation(index_path, O_RDWR | O_LARGEFILE | O_CREAT);
			is_load_ = false;
		}
	
		IndexHandle::~IndexHandle()
		{
			if(file_op_)
			{
				delete file_op_;
				file_op_ = NULL;
			}
		}
		
		int IndexHandle::create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(debug) printf("create index, block id：%u, bucket size：%d, max_mmap_size：%d, \
				first mmap size：%d, per mmap size：%d\n", logic_block_id, bucket_size, 
				map_option.max_mmap_size_, map_option.first_mmap_size_, map_option.per_mmap_size_);
			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();    //获取文件大小
			if(file_size < 0)
			{
				return TFS_ERROR;
			}
			else if(file_size == 0)  //empty file
			{
				IndexHeader i_header;
				i_header.block_info.block_id_ = logic_block_id;
				i_header.block_info.seq_no_ = 1;
				i_header.bucket_size_ = bucket_size;
				i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
				
				//index header + total buckets
				char* init_data = new char[i_header.index_file_size_];         //free
				memcpy(init_data, &i_header, sizeof(IndexHeader));
				memset(init_data + sizeof(IndexHeader), 0, i_header.index_file_size_ - sizeof(IndexHeader));
				
				//wirte IndexHeader and buckets into index file
				ret = file_op_->pwrite_file(init_data, i_header.index_file_size_, 0);
				
				delete[] init_data;
				init_data = NULL;
				
				if(ret != TFS_SUCCESS)
				{
					return ret;
				}
				
				ret = file_op_->flush_file();
				
				if(ret != TFS_SUCCESS)
				{
					return ret;
				}
			}
			else    //file_size > 0, index file already exist
			{		
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}
			
			ret = file_op_->mmap_file(map_option);           //mmap_file
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			is_load_ = true;
			
			if(debug) printf("init block id：%d index successful. data file size：%d, index file size：%d, bucket_size：%d, \
				free head offset：%d, seq_no：%d, size：%d, file count：%d, del_size：%d, del_file_count：%d, version：%d\n", 
				logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_, index_header()->bucket_size_, 
				index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_t_, block_info()->file_count_, 
				block_info()-> del_size_, block_info()->del_file_count_, block_info()->version_);
			
			return TFS_SUCCESS;
		}
		
		int IndexHandle::load(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption& map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();
			if(file_size < 0)
			{
				return file_size;
			}
			else if(file_size == 0)  //empty file
			{
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			MMapOption tmp_map_option = map_option;
			
			if(file_size > tmp_map_option.first_mmap_size_ && file_size <= tmp_map_option.max_mmap_size_)
			{
				tmp_map_option.first_mmap_size_ = file_size;
			}
			
			ret = file_op_->mmap_file(tmp_map_option);               //mmap_file
			
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			//int32_t bct_size = bucket_size();
			if(0 == bct_size() || 0 == block_info()->block_id_)
			{
				fprintf(stderr, "Index corrupt error. block id：%u, bucket size：%d\n", block_info()->block_id_, bct_size());
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check file size
			int32_t index_file_size = sizeof(IndexHeader) + bct_size() * sizeof(int32_t);
			
			if(file_size < index_file_size)
			{
				fprintf(stderr, "Index corrupt error, block id：%u, bucket size：%d, file size：%" __PRI64_PREFIX"d, index file size：%d\n", 
					block_info()->block_id_, bct_size(), file_size, index_file_size);
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check block id
			if(logic_block_id != block_info()->block_id_)
			{
				fprintf(stderr, "block id conflict. block id ：%u, index block id：%u\n", logic_block_id, block_info()->block_id_);
				return EXIT_BLOCKID_CONFLICT_ERROR;
			}
			
			//check bucket size
			if(bucket_size != bct_size())
			{
				fprintf(stderr, "Index configure error, old bucket size：%d, new bucket size：%d", bucket_size, bct_size());
				return EXIT_BUCKET_CONFIGURE_ERROR;
			}
			
			is_load_ = true;
			
			if(debug) printf("load block id：%d index successful. data file size：%d, index file size：%d, bucket_size：%d, \
				free head offset：%d, seq_no：%d, size：%d, file count：%d, del_size：%d, del_file_count：%d, version：%d\n", 
				logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_, index_header()->bucket_size_, 
				index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_t_, block_info()->file_count_, 
				block_info()-> del_size_, block_info()->del_file_count_, block_info()->version_);
			
			return TFS_SUCCESS;
		}
		
		int IndexHandle::remove(const uint32_t logic_block_id)
		{
			if(is_load_)
			{		
				if(logic_block_id != block_info()->block_id_)
				{
					fprintf(stderr, "block id conflict. block id：%d, index block id：%d\n", logic_block_id, block_info()->block_id_);
					return EXIT_BLOCKID_CONFLICT_ERROR;
				}
			}
			
			int ret = file_op_->munmap_file();
			if(TFS_SUCCESS != ret)
			{
				return ret;
			}
			
			ret = file_op_->unlink_file();
			return ret;
		}
		
		int IndexHandle::flush()
		{
			int ret = file_op_->flush_file();
			if(TFS_SUCCESS != ret)
			{
				fprintf(stderr, "index flush fail, ret：%d, error desc：%s\n", ret, strerror(errno));
			}
			return ret;
		}
		
		int IndexHandle::update_block_info(const OperType oper_type, const uint32_t modify_size)
		{
			if(block_info()->block_id_ == 0)
			{
				return EXIT_BLOCKID_ZERO_ERROR;
			}
			
			if(oper_type == C_OPER_INSERT)
			{
				++block_info()->version_;
				++block_info()->file_count_;
				++block_info()->seq_no_;
				block_info()->size_t_ += modify_size;	
			}
			else if(oper_type == C_OPER_DELETE)
			{
				++block_info()->version_;
				--block_info()->file_count_;
				block_info()->size_t_ -= modify_size;
				++block_info()->del_file_count_;
				block_info()->del_size_ += modify_size;
			}
			
			if(debug) printf("update block info.blockid：%u, version：%u, file count：%u, size：%u, del file count：%u, del size：%u, seq no：%u, oper type：%d\n",
						block_info()->block_id_, block_info()->version_, block_info()->file_count_, block_info()->size_t_, block_info()->del_file_count_, block_info()->del_size_, 
						block_info()->seq_no_, oper_type);
			
			return TFS_SUCCESS;
		}
		
		int32_t IndexHandle::write_segment_meta(const uint64_t key, MetaInfo& meta)
		{
			int32_t current_offset = 0, previous_offset = 0;         //当前读取的偏移量   //当前读取的前一个的偏移量
			
			//* key 是否存在？存在->处理？  不存在->处理？
			//1.从文件哈希表中查找key是否存在   hash_find(key, current_offset, previous_offset);
			int32_t ret = hash_find(key, current_offset, previous_offset);
			
			if(TFS_SUCCESS == ret)           //查找到哈希链表中该key已经存在
			{
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}
			else if(EXIT_META_NOT_FOUND_ERROR != ret)     //not found key(状态)
			{
				return ret;
			}
				
			//2.如果不存在就写入meta到文件哈希表中 hash_insert(key, previous_offset, meta)
			ret = hash_insert(key, previous_offset, meta);
			
			return ret;
		}
		
		int32_t IndexHandle::read_segment_meta(const uint64_t key, MetaInfo& meta)
		{
			int32_t current_offset = 0, previous_offset = 0;         //当前读取的偏移量   //当前读取的前一个的偏移量
			
			int32_t ret = hash_find(key, current_offset, previous_offset);
			
			if(TFS_SUCCESS == ret)      //exist
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&meta), sizeof(MetaInfo), current_offset);
				return ret;
			}
			else
			{
				return ret;
			}			
		}
		
		int32_t IndexHandle::delete_segment_meta(const uint64_t key)
		{
			int32_t current_offset = 0, previous_offset = 0;
			
			int32_t ret = hash_find(key, current_offset, previous_offset);
			
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			MetaInfo meta_info;
			ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), current_offset);
			if(TFS_SUCCESS != ret)
			{
				return ret;
			}
			
			int32_t next_pos = meta_info.get_next_meta_offset();          //下一个节点在链表中的偏移量
			meta_info.set_key(-1);
			
			if(previous_offset == 0)
			{
				int32_t slot = static_cast<int32_t>(key) % bct_size();
				bucket_slot()[slot] = next_pos;
				
			}
			else
			{
				MetaInfo pre_meta_info;
				ret = file_op_->pread_file(reinterpret_cast<char*>(&pre_meta_info), sizeof(MetaInfo), previous_offset);
				if(TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				pre_meta_info.set_next_meta_offset(next_pos);
				
				ret = file_op_->pwrite_file(reinterpret_cast<char*>(&pre_meta_info), sizeof(MetaInfo), previous_offset);         
				if(TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				
			}	
			
			//把删除节点加入可重用节点链表       
			//前插法
			meta_info.set_next_meta_offset(free_head_offset());      //index_header()->free_head_offset_       
			ret = file_op_->pwrite_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), current_offset);
			if(TFS_SUCCESS != ret)
			{
				return ret;
			}
			
			index_header()->free_head_offset_ = current_offset;
			
			if(debug) printf("delete_segment_meta - reuse metainfo, current_offset：%d\n", current_offset);
			
			update_block_info(C_OPER_DELETE, meta_info.get_size());
				
			return TFS_SUCCESS;
		}
		
		int32_t IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset)
		{
			int ret = TFS_SUCCESS;
			MetaInfo meta_info;      //保存临时读到的metainfo
			
			current_offset = 0;          //当前读取的偏移量
			previous_offset = 0;         //当前读取的前一个的偏移量
			
			//1.确定key存放的桶（slot）的位置
			int32_t slot = static_cast<int32_t>(key) % bct_size();
			
			//2.读取首节点存储的第一个节点的偏移量，如果偏移量为零，直接返回 EXIT_META_NOT_FOUND_ERROR
			//3.根据偏移量读取存储的metainfo
			//4.与key进行比较，相等则设置current_offset 和 previous_offset并返回TFS_SUCCESS，否则继续执行5
			//5.从metainfo中取得下一个节点的在文件中的偏移量，如果偏移量位零，直接返回 EXIT_META_NOT_FOUND_ERROR，否则，跳转至3继续循环执行
			int32_t pos = bucket_slot()[slot];
			
			for(; pos != 0; )
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), pos);
				if(TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				if(hash_compare(key, meta_info.get_key()))        
				{
					current_offset = pos;
					return TFS_SUCCESS;
				}
			
				previous_offset = pos;
				pos = meta_info.get_next_meta_offset();
			}
			
			return EXIT_META_NOT_FOUND_ERROR;
		}
		
		int32_t IndexHandle::hash_insert(const uint64_t key, int32_t previous_offset, MetaInfo& meta)
		{
			int ret = TFS_SUCCESS;
			int32_t current_offset = 0;
			MetaInfo tmp_meta_info;    //保存临时读到的metainfo
			
			//1.确定key存放的桶（slot）的位置
			int32_t slot = static_cast<int32_t>(key) % bct_size();
			
			//2.确定meta节点存储在文件中的偏移量
			if(free_head_offset() != 0)
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), free_head_offset());
				if(TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				current_offset = index_header()->free_head_offset_;
				
				if(debug) printf("reuse metainfo, current_offset：%d\n", current_offset);
				
				index_header()->free_head_offset_ = tmp_meta_info.get_next_meta_offset();
				
			}
			else
			{
				current_offset = index_header()->index_file_size_;
				index_header()->index_file_size_ += sizeof(MetaInfo);
			}
			
			//3.将meta节点写入索引文件中
			meta.set_next_meta_offset(0);
			
			ret = file_op_->pwrite_file(reinterpret_cast<const char*>(&meta), sizeof(MetaInfo), current_offset);
			if(TFS_SUCCESS != ret)
			{
				index_header()->index_file_size_ -= sizeof(MetaInfo);
				return ret;
			}
			
			//4.将meta节点插入到哈希链表中
			if(0 != previous_offset)       //前一个节点已经存在
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
				if(TFS_SUCCESS != ret)
				{
					index_header()->index_file_size_ -= sizeof(MetaInfo);
					return ret;
				}
				
				tmp_meta_info.set_next_meta_offset(current_offset);
				ret = file_op_->pwrite_file(reinterpret_cast<const char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
				if(TFS_SUCCESS != ret)
				{
					index_header()->index_file_size_ -= sizeof(MetaInfo);
					return ret;
				}
			}
			else        //不存在前一个节点，为首节点
			{
				bucket_slot()[slot] = current_offset;
			}
			
			return TFS_SUCCESS;
		}
		
		int32_t IndexHandle::block_tidy(FileOperation* fo)                //整理块
		{
			//查看del_file_count
			//根据文件编号，逐步从头部开始写， hash_find 没有就continue，有就往前写
			//截断文件
			//更新索引文件信息
			
			if(!fo)          //块文件不存在
			{
				return EXIT_BLOCK_NOT_EXIST;
			}
			
			if(block_info()->del_file_count_ <= 0)        //块删除文件数量小于0
			{
				fprintf(stderr, "block id %u do not have del_file. del_file_count：%d\n", block_info()->block_id_, block_info()->del_file_count_);
				return EXIT_BLOCK_DEL_FILE_COUNT_LESSZERO;
			}
			
			if(block_info()->del_size_ <= 0)             //块删除文件大小小于0
			{
				fprintf(stderr, "block id %u do not have del_file_size. del_file_size：%d\n", block_info()->block_id_, block_info()->del_size_);
				return EXIT_BLOCK_DEL_SIZE_LESSZEOR;
			}
			
			int32_t file_count = block_info()->file_count_;        //文件数量
			int32_t ret = TFS_SUCCESS;
			int32_t over_write_offset = 0;       //整个文件写入块后的偏移量
			int32_t current_write_offset = 0;    //文件未写全，块中的偏移量
		
			int64_t residue_bytes = 0;          //写入后还剩下需要写的字节数
			uint64_t key = 1;         //保存文件编号
			
			
			//整理块
			for(int i = 1; i <= file_count; )
			{
				MetaInfo meta_info;            //保存临时读到的metainfo
				char buffer[4096] = { '0' };                 //保存的文件
				int nbytes = sizeof(buffer);        //该次需要写入的字节数
				
				ret = read_segment_meta(key, meta_info);
				
				current_write_offset = meta_info.get_offset();   
				residue_bytes = meta_info.get_size(); 
			
				if(debug) fprintf(stderr, "i：%d, file_id：%ld, key：%ld, ret：%d\n", i, meta_info.get_key(), key, ret);
				
				if(TFS_SUCCESS == ret)           //已经在哈希链表中读到
				{
					
					if(meta_info.get_size() <= sizeof(buffer))        //一次读完
					{	
						ret = fo->pread_file(buffer, meta_info.get_size(), meta_info.get_offset());      
						if(ret == TFS_SUCCESS)    //文件读成功,将文件重新写入块中
						{
							ret = fo->pwrite_file(buffer, meta_info.get_size(), over_write_offset);
							if(ret == TFS_SUCCESS)          //文件写入成功
							{
								over_write_offset += meta_info.get_size();
								key++;
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
						nbytes = sizeof(buffer);
						
						for(int j = 0; j < 1; )
						{
							ret = fo->pread_file(buffer, nbytes, current_write_offset);     
							if(ret == TFS_SUCCESS)    		//文件读成功,将部分文件重新写入块中
							{
								//fprintf(stderr, "nbytes：%d\n", nbytes);
								ret = fo->pwrite_file(buffer, nbytes, over_write_offset);
								if(ret == TFS_SUCCESS)          //文件写入成功
								{
									current_write_offset += nbytes;
									over_write_offset += nbytes;
									residue_bytes -= nbytes;
									
									//fprintf(stderr, "residue_bytes：%ld\n", residue_bytes);
									
									if(0 == residue_bytes) 
									{
										key++;       
										j++;          //结束循环
										continue;
									}
									
									if(nbytes > residue_bytes)
									{	
										nbytes = residue_bytes;
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
			}
			
			ret = fo->flush_file();
			//截断文件
			ret = fo->ftruncate_file(block_info()->size_t_);
			
			//更新索引文件信息
			index_header()->data_file_offset_ = block_info()->size_t_;
			
			//更新block info
			ret = block_info()->del_file_count_ = 0;
			ret = block_info()->del_size_ = 0;
			flush();
			
			return TFS_SUCCESS;
		}
				
	}
		
}