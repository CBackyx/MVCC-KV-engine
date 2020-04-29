#include "headers.h"

using namespace std;
using namespace std::chrono;

string threadFiles[MAX_THREAD_NUM];
int threadIDs[MAX_THREAD_NUM];
thread ths[MAX_THREAD_NUM];
bool x_is_active[MAX_TRANSACTION_NUM];

Engine *engine;


int doThread(int id) {
    int cur_v = 0;
    int cur_xid = 0;
    char cmd_buffer[MAX_COMMAND_SIZE];
    char tar[MAX_KEY_SIZE];
    char cur_op;
    int cur_op_num;
    string cur_k;

    FILE *fp;
    fp = fopen(threadFiles[id].c_str(), "r");
    if (fp == NULL) {
        printf("Open input file for thread %d failed!\n", id);
        return -1;
    }

    FILE *wfp;
    char ofNameBuffer[100];
    sprintf(ofNameBuffer, "output_thread_%d.csv", threadIDs[id]);
    wfp = fopen(ofNameBuffer, "w");
    if (wfp == NULL) {
        printf("Open output file for thread %d failed!\n", id);
        return -1;
    }

    fprintf(wfp, "transaction_id,type,time,value");

    time_t cur_t;

    while (fscanf(fp, "%c", &cmd_buffer) != EOF) {
        switch (cmd_buffer[0]) {
            case 'B':
                // Begin
                fscanf(fp, "%d", &cur_xid);
                x_is_active[cur_xid] = true;
                cur_t = system_clock::to_time_t(system_clock::now());
                fprintf(wfp, "%d,BEGIN,%s,", cur_xid, ctime(&cur_t));
                break;
            case 'C':
                // Commit
                fscanf(fp, "%d", &cur_xid);
                x_is_active[cur_xid] = false;
                cur_t = system_clock::to_time_t(system_clock::now());
                fprintf(wfp, "%d,END,%s,\n", cur_xid, ctime(&cur_t));
                break;
            case 'R':
                // Read
                fscanf(fp, "%s", tar);
                cur_k = tar;
                cur_v = engine->getValue(cur_k, cur_xid);
                if (cur_v == INT_MIN) printf("Read failed!\n");
                cur_t = system_clock::to_time_t(system_clock::now());
                fprintf(wfp, "%d,%s,%s,%d\n", cur_xid, cur_k, ctime(&cur_t), cur_v);
                break;
            case 'S':
                // Set
                fscanf(fp, "%s, %s %c %d\n", tar, tar, &cur_op, &cur_op_num);
                cur_k = tar;
                cur_v = engine->getValue(cur_k, cur_xid);
                if (cur_v == INT_MIN) printf("Read failed when updating!\n");
                if (cur_op == '+') cur_v += cur_op_num;
                else cur_v -= cur_op_num;
                while (engine->updateRecord(cur_k, cur_v, cur_xid) != 0) {
                    printf("Update blocked!\n");
                    std::this_thread::sleep_for (std::chrono::microseconds(1));
                }
                engine->reclaim(cur_k); 
                break;
            default:
                printf("Unrecognized command!\n");
                exit(-1);
        }

        return 0;
    }
    
    fclose(fp);
    fclose(wfp);
    return 0;
}

int main() {
    // Initialize the KV engine
    engine = new Engine();

    char *threadFilesPath = ".//input//thread_*.txt";
    int threadNum = getFiles(threadFilesPath, threadFiles);

    for (int i = 0; i < threadNum; ++i) {
        sscanf(threadFiles[i].c_str(), "thread_%d.txt", &threadIDs[i]);
    }

    printf("Thread num is %d\n", threadNum);
    // Initialize transactions info
    for (int i = 0; i < MAX_TRANSACTION_NUM; ++ i) {
        x_is_active[i] = false;
    }
    
    // Now initialize the KV engine using "data_prepare.txt"
    FILE *fp;
    fp = fopen("data_prepare.txt", "r");
    if (fp == NULL) {
        printf("Open data_prepare.txt failed!\n");
        return -1;
    }

    // Attention!
    // The 0 transaction is preserved for initialize the KV, so it is always not active
    int cur_v = 0;
    char cmd_buffer[MAX_COMMAND_SIZE];
    while (fscanf(fp, "INSERT %s %d", cmd_buffer, cur_v) != EOF) {
        string curs = cmd_buffer;
        engine->addRecord(curs, cur_v, 0);
    }

    fclose(fp);

    // Do all the threads
    for (int i = 0; i < threadNum; ++i) {
        ths[i] = thread(doThread, i);
    }
    for (int i = 0; i < threadNum; ++i) {
        ths[i].join();
    }

    // Output the final KV state
    FILE *fp;
    fp = fopen("final_state.csv", "w");
    if (fp == NULL) {
        printf("Open final_state.txt failed!\n");
        return -1;
    }

    for (std::map<std::string, struct RecordLine*>::iterator it = engine->table.begin(); it != engine->table.end(); ++it) {
        string cur_k = it->first;
        struct RecordLine* currl = it->second;
        if (currl->records[0].createdXid != -1) {
            fprintf(fp, "%s,%d", cur_k.c_str(), currl->records[0].value);
        } else if (currl->records[1].createdXid != -1) {
            fprintf(fp, "%s,%d", cur_k.c_str(), currl->records[1].value);
        } else continue;
    }

    fclose(fp);

    delete engine;

    return 0;
}