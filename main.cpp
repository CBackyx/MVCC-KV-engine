#include "headers.h"

using namespace std;

string threadFiles[MAX_THREAD_NUM];
thread ths[MAX_THREAD_NUM];
bool x_is_active[MAX_TRANSACTION_NUM];

int doThread(int id) {
    FILE *fp;
    fp = fopen(threadFiles[id].c_str(), "r");
    if (fp == NULL) return -1; 
    char tmp = 0;
    char read = 'r';
    unsigned long long tr = 0;
    printf("h3\n");
    while (fscanf(fp, "%c", &tmp) != EOF) {
        if (tmp == read) {
            this->is_read[cnt] = true;
            // printf("r ");
        }
        fscanf(fp, "%llx", &this->addrs[cnt]);
        fscanf(fp, "%llx", tr);
        // printf("%llx\n", this->addrs[cnt]);
        cnt++;
    }
    fclose(fp);
    return 0;
}

int main() {
    char *threadFilesPath = ".//input//thread_*.txt";
    int threadNum = getFiles(threadFilesPath, threadFiles);
    printf("Thread num is %d\n", threadNum);
    // Initialize transactions info
    for (int i = 0; i < MAX_TRANSACTION_NUM; ++ i) {
        x_is_active[i] = false;
    }
    
    // Now initialize the KV engine using "data_prepare.txt"

    //

    for (int i = 0; i < threadNum; ++i) {
        ths[i] = thread(doThread, i);
    }
    for (int i = 0; i < threadNum; ++i) {
        ths[i].join();
    }

    return 0;
}