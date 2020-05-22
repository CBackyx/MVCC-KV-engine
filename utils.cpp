#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <vector>
#include <string>
#include <iostream>

using namespace std; 

// getFiles(".//input//thread_*.txt");
int getFiles(const char *dir, string *fileNames)
{
    int fileNum = 0;

    DIR *dp;
    struct dirent *dirp;

    if((dp = opendir(dir)) == NULL)
    {
      cout << "Error(" << errno << ") opening " << dir << endl;
      return errno;
    }
    while ((dirp = readdir(dp)) != NULL) {
        string curf = string(dirp->d_name);
        // printf("%s\n", curf.c_str());
        if (curf.find("thread_1") != std::string::npos) {
            fileNames[fileNum] = curf;
            fileNum++;
        }
    }
      
    closedir(dp);
    
    return fileNum; 
}
