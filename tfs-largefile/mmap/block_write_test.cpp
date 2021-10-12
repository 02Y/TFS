#include "common.h"
#include "file_op.h"
#include "index_handle.h"
#include <sstream>

using namespace std;
using namespace conway;
using namespace largefile;


static const MMapOption mmap_option = {1024000, 4096, 4096};    //内存映射参数
static const uint32_t main_blocksize = 1024 * 1024 * 64;        //主块文件的大小
static const uint32_t bucket_size = 1000;   //哈希桶的大小
static int32_t block_id = 1;

static int debug = 1;

int main(int argc, char** argv)     //比如：argv[0] = "rm"  argv[1] = "-f"  argv[2] = "a.out"   此时  argc = 3
{
	string mainblock_path;
	string index_path;
	int32_t ret = TFS_SUCCESS;	
	
	cout << "type your block id：";
	cin >> block_id;
	
	if(block_id < 1)
	{
		cerr << "Invalid block id, exit\n";
		exit(-1);
	}	
	
	//1.加载索引文件
	IndexHandle* index_handle = new IndexHandle(".", block_id);   //索引文件句柄    //free
	
	if(debug) printf("load index ...\n");
	
	ret = index_handle->load(block_id, bucket_size, mmap_option);
	
	if(ret != TFS_SUCCESS)
	{
		fprintf(stderr, "load index %d faild.\n", block_id);
		//delete mainblock;
		delete index_handle;
		exit(-2);
	}	

	//2.写入文件到主块文件中
	stringstream tmp_stream;
	tmp_stream << "." << MAINBLOCK_DIR_PREFIX << block_id;
	tmp_stream >> mainblock_path;
	
	FileOperation* mainblock = new FileOperation(mainblock_path, O_RDWR | O_LARGEFILE | O_CREAT);      //free
	
	char buffer[8000];
	memset(buffer, '4', sizeof(buffer));
	
	int32_t data_offset = index_handle->get_block_data_offset();      //offset to write next data in block
	uint32_t file_no = index_handle->block_info()->seq_no_;           //next available file number
	
	if((ret = mainblock->pwrite_file(buffer, sizeof(buffer), data_offset)) != TFS_SUCCESS)
	{
		fprintf(stderr, "write to mian block failed. ret：%d, reason：%s\n", ret, strerror(errno));
		mainblock->close_file();
		
		delete mainblock;
		delete index_handle;
		exit(-3);
	}
	
	//3.索引文件中写入MetaInfo
	MetaInfo meta;
	meta.set_file_id(file_no);           //文件编号
	meta.set_offset(data_offset);        //文件在主块内部的偏移量
	meta.set_size(sizeof(buffer));       //文件大小
	
	ret = index_handle->write_segment_meta(meta.get_key(), meta);
	if(ret == TFS_SUCCESS)         //写入成功
	{
		//1.更新索引头部信息
		index_handle->commit_block_data_offset(sizeof(buffer));
		//2.更新块信息
		index_handle->update_block_info(C_OPER_INSERT, sizeof(buffer));
		
		ret = index_handle->flush();
		
		if(ret != TFS_SUCCESS)
		{
			fprintf(stderr, "flush main block %d failed. file no：%u\n", block_id, file_no);
		}
		
	}
	else         //没有写入成功
	{
		fprintf(stderr, "write_segment_meta - main block %d failed. file no：%u\n", block_id, file_no);
	}
	
	if(ret != TFS_SUCCESS)
	{
		fprintf(stderr, "write to main block %d failed. file no：%u\n", block_id, file_no);
	}
	else
	{
		if(debug) printf("write successfully. file no：%u, block id：%d\n", file_no, block_id);
	}
	
	mainblock->close_file();
	
	delete mainblock;
	delete index_handle;
	
	return 0;
}
	
	
	
	
	
	
	
	
	
	
	
	
