#include "mmap_file.h"      //main test

using namespace std;
using namespace conway;
using namespace largefile;

static const mode_t OPEN_MODE = 0644;
static const MMapOption mmap_option = {10240000, 4096, 4096};   //内存映射的参数

int open_file(string file_name, int open_flags)
{
	int fd = ::open(file_name.c_str(), open_flags, OPEN_MODE); //open成功返回的一定>0
	if(fd < 0)
	{
		return -errno;  //errno是正数 strerror(errno)获取原因
	}
	
	
	return fd;
}

int main(void)
{
	const char* file_name = "./mapfile_test.txt";
	
	//1.打开/创建一个文件，取得文件的句柄  open函数
	int fd = open_file(file_name, O_RDWR|O_LARGEFILE);
	
	if(fd < 0)
	{
		//调用read，出错重置 errno
		fprintf(stderr, "open file failed.filename：%s, error desc：%s\n", 
		file_name, strerror(-fd)); //相当于strerror(errno)
		
		return -1;
	}
	
	//2.创建文件映射对象
	MMapFile* map_file = new MMapFile(mmap_option, fd);
	
	bool is_mmaped = map_file->map_file(true);
	
	if(is_mmaped)
	{
		map_file->mremap_file();
		
		memset(map_file->get_data(), '8', map_file->get_size());
		map_file->sync_file(); //同步操作
		map_file->munmap_file();
	}
	else
	{
		fprintf(stderr, "map file failed\n");
	}
	
	::close(fd);
	
	return 0;
}