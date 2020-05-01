#include "headers.h"

using namespace std; 

// getFiles(".//input//thread_*.txt");
int getFiles(const char *dir, string *fileNames)
{
    intptr_t handle;
    _finddata_t findData;

    handle = _findfirst(dir, &findData);
    if (handle == -1)
    {
        cout << "Failed to find first file!\n";
        return -1;
    }

    int fileNum = 0;

    do
    {
        fileNames[fileNum] = findData.name;
        cout << fileNames[fileNum] << endl;
        ++ fileNum;
    } while (_findnext(handle, &findData) == 0);

    cout << "get Files Done!\n";
    _findclose(handle);
    
    return fileNum; 
}