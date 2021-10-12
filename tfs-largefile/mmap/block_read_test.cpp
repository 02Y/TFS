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

	//2.读取文件的metainfo
	uint64_t file_id = 0;
	cout << "type your file id：";
	cin >> file_id;
	
	
	if(file_id < 1)
	{
		cerr << "Invalid file id, exit\n";
		exit(-3);
	}
	
	MetaInfo meta;
	ret = index_handle->read_segment_meta(file_id, meta);
	if(ret != TFS_SUCCESS)
	{
		fprintf(stderr, "read_segment_meta error. file_id：%ld, ret：%d\n", file_id, ret);
		exit(-4);
	}
	
	//3.根据meta info读取文件
	stringstream tmp_stream;
	tmp_stream << "." << MAINBLOCK_DIR_PREFIX << block_id;
	tmp_stream >> mainblock_path;
	
	FileOperation* mainblock = new FileOperation(mainblock_path, O_RDWR);      //free
	char* buffer = new char[meta.get_size() + 1];                              //free
	
	ret = mainblock->pread_file(buffer, meta.get_size(), meta.get_offset());
	if(ret != TFS_SUCCESS)
	{
		fprintf(stderr, "read from mian block failed. ret：%d, reason：%s\n", ret, strerror(errno));
		mainblock->close_file();
		
		delete mainblock;
		delete index_handle;
		exit(-5);
	}

	buffer[meta.get_size()] = '\0';
	printf("read：%s\n read sizeof %d\n", buffer, meta.get_size());

	mainblock->close_file();
	delete mainblock;
	delete buffer;
	delete index_handle;
	
	return 0;
}
