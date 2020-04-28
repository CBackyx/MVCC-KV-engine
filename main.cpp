#include "headers.h"

using namespace std;

string threadFiles[MAX_THREAD_NUM];

int main() {
    char *threadFilesPath = ".//input//thread_*.txt";
    int threadNum = getFiles(threadFilesPath, threadFiles);
    printf("Thread num is %d\n", threadNum);
    return 0;
}