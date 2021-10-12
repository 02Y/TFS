#include "file_op.h"         //main test
#include "common.h"

using namespace std;
using namespace conway;
using namespace largefile;

int main(void)
{
	const char* filename = "file_op.txt";
	
	FileOperation* fileOP = new FileOperation(filename, O_RDWR | O_CREAT | O_LARGEFILE);
	
	int fd = fileOP->open_file();
	if(fd < 0)
	{
		fprintf(stderr, "open file %s failed.reason %s\n", filename, strerror(-fd));
		exit(-1);
	}
	
	char buffer[65];
	memset(buffer, '8', 64);
	
	int ret = fileOP->pwrite_file(buffer, 64, 128);
	if(ret < 0)
	{
		if(ret == EXIT_DISK_OPER_INCOMPLETE)
		{
			fprintf(stderr, "pwrite_file：read length is less than required.");
		}
		else
		{
			fprintf(stderr, "pwrite file %s failed.reason %s\n", filename, strerror(-ret));
		}
	}
	
	memset(buffer, 0, 64);
	ret = fileOP->pread_file(buffer, 64, 128);
	if(ret < 0)
	{
		if(ret == EXIT_DISK_OPER_INCOMPLETE)
		{
			fprintf(stderr, "pread_file：read length is less than required.");
		}
		else
		{
			fprintf(stderr, "pread file %s failed.reason %s\n", filename, strerror(-ret));
		}
	}
	else
	{
		buffer[64] = '\0';
		printf("read：%s\n", buffer);
	}
	
	memset(buffer, '9', 64);
	ret = fileOP->write_file(buffer, 64);     
	if(ret < 0)
	{
		if(ret == EXIT_DISK_OPER_INCOMPLETE)
		{
			fprintf(stderr, "write：read length is less than required.");
		}
		else
		{
			fprintf(stderr, "write file %s failed.reason %s\n", filename, strerror(-ret));
		}
	}
	
	fileOP->close_file();
	
	return 0;
}