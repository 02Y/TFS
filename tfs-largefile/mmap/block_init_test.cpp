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
	
	//1.创建索引文件
	IndexHandle* index_handle = new IndexHandle(".", block_id);   //索引文件句柄    //free
	
	if(debug) printf("Init index ...\n");
	
	ret = index_handle->create(block_id, bucket_size, mmap_option);
	
	if(ret != TFS_SUCCESS)
	{
		fprintf(stderr, "create index %d faild.\n", block_id);
		//elete mainblock;
		delete index_handle;
		exit(-3);
	}	

	//2.生成主块文件
	stringstream tmp_stream;
	tmp_stream << "." << MAINBLOCK_DIR_PREFIX << block_id;
	tmp_stream >> mainblock_path;
	
	FileOperation* mainblock = new FileOperation(mainblock_path, O_RDWR | O_LARGEFILE | O_CREAT);      //free
	
	ret = mainblock->ftruncate_file(main_blocksize);
	
	if(ret != 0)
	{
		fprintf(stderr, "create main block %s faild. reason：%s\n", mainblock_path.c_str(), strerror(errno));
		delete mainblock;
		index_handle->remove(block_id);
		exit(-2);
	}	
	
	//其他操作
	mainblock->close_file();
	index_handle->flush();
	
	delete mainblock;
	delete index_handle;
	
	return 0;
}
	
	
	
	
	
	
	
	
	
	
	
	
