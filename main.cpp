#include "headers.h"

using namespace std;
using namespace std::chrono;

string threadFiles[MAX_THREAD_NUM];
int threadIDs[MAX_THREAD_NUM];
thread ths[MAX_THREAD_NUM];
bool x_is_active[MAX_TRANSACTION_NUM + 1];

Engine *engine;

time_point<high_resolution_clock> start;

int doThread(int id);
double getTime();

int doThread(int id) {
    int cur_v = 0;
    int cur_xid = 0;
    char cmd_buffer[MAX_COMMAND_SIZE];
    char tar[MAX_KEY_SIZE];
    char cur_op;
    int cur_op_num;
    string cur_k;

    FILE *fp;
    fp = fopen(threadFiles[id].c_str(), "rb");
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

    fprintf(wfp, "transaction_id,type,time,value\n");

    time_t cur_t;
    vector<string> toReclaim;

    while (fscanf(fp, "%s", cmd_buffer) != EOF) {
        // printf("shit thread %d\n", id);
        switch (cmd_buffer[0]) {
            case 'B':
                // Begin
                fscanf(fp, "%d", &cur_xid);
                x_is_active[cur_xid] = true;
                fprintf(wfp, "%d,BEGIN,%llf,\n", cur_xid, getTime());
                printf("Thread %d Begin transaction %d\n", id, cur_xid);
                break;
            case 'C':
                // Commit
                fscanf(fp, "%d", &cur_xid);
                x_is_active[cur_xid] = false;
                fprintf(wfp, "%d,END,%llf,\n", cur_xid, getTime());
                printf("Thread %d End transaction %d\n", id, cur_xid);
                // Reclaim the resources
                for (auto x:toReclaim) {
                    engine->reclaim(x);
                } 
                break;
            case 'R':
                // Read
                fscanf(fp, "%s", tar);
                cur_k = tar;
                cur_v = engine->getValue(cur_k, cur_xid);
                while (cur_v == INT_MIN) {
                    printf("Read failed!\n");
                    cur_v = engine->getValue(cur_k, cur_xid);
                }
                fprintf(wfp, "%d,%s,%llf,%d\n", cur_xid, tar, getTime(), cur_v);
                printf("Thread %d Read %s %d\n", id, tar, cur_v);
                break;
            case 'S':
                // Set
                fscanf(fp, "%s", tar);
                fscanf(fp, "%s %c %d", tar, &cur_op, &cur_op_num);
                // printf("%s, ++ %s ++ %c ++ %d\n", tar, tar, cur_op, cur_op_num);
                cur_k = tar;
                cur_v = engine->getValue(cur_k, cur_xid);
                while (cur_v == INT_MIN) {
                    printf("Read failed when updating!\n");
                    cur_v = engine->getValue(cur_k, cur_xid);
                }
                if (cur_op == '+') cur_v += cur_op_num;
                else cur_v -= cur_op_num;
                while (engine->updateRecord(cur_k, cur_v, cur_xid) != 0) {
                    printf("Update blocked!\n");
                    std::this_thread::sleep_for (std::chrono::microseconds(1));
                }
                printf("Thread %d Set %s %d\n", id, tar, cur_v);
                toReclaim.push_back(cur_k);
                break;
            default:
                printf("Unrecognized command!\n");
                exit(-1);
        }

    }
    
    fclose(fp);
    fclose(wfp);
    return 0;
}

// Actually this is a duration since start point
double getTime() {
    time_point<high_resolution_clock> end;
    double duration;
    end = high_resolution_clock::now();//获取当前时间
	auto dur = duration_cast<nanoseconds>(end - start);
	duration = double(dur.count()) * nanoseconds::period::num / nanoseconds::period::den * 1000000000;
    return duration;
}

int main() {
    // Initialize the KV engine
    engine = new Engine();

    char threadFilesPath[] = ".//thread_*.txt";
    int threadNum = getFiles(threadFilesPath, threadFiles);

    for (int i = 0; i < threadNum; ++i) {
        sscanf(threadFiles[i].c_str(), "thread_%d.txt", &threadIDs[i]);
    }

    printf("Thread num is %d\n", threadNum);
    // Initialize transactions info
    for (int i = 0; i < MAX_TRANSACTION_NUM + 1; ++ i) {
        x_is_active[i] = false;
    }
    
    // Now initialize the KV engine using "data_prepare.txt"
    printf("Initializing KV engine content\n");
    FILE *fp;
    fp = fopen("data_prepare.txt", "r");
    if (fp == NULL) {
        printf("Open data_prepare.txt failed!\n");
        return -1;
    }

    // Attention!
    // The MAX_TRANSACTION_NUM transaction is preserved for initialize the KV, so it is always not active
    int cur_v = 0;
    char cmd_buffer[MAX_COMMAND_SIZE];
    while (fscanf(fp, "%s", cmd_buffer) != EOF) {
        fscanf(fp, "%s", cmd_buffer);
        fscanf(fp, "%d", &cur_v);
        string curs = cmd_buffer;
        cout << curs << endl;
        engine->addRecord(curs, cur_v, MAX_TRANSACTION_NUM);
    }

    fclose(fp);

    start = high_resolution_clock::now();

    // Do all the threads
    printf("Initializing threads!\n");
    for (int i = 0; i < threadNum; ++i) {
        ths[i] = thread(doThread, i);
    }
    for (int i = 0; i < threadNum; ++i) {
        ths[i].join();
    }

    // Output the final KV state
    fp = fopen("final_state.csv", "w");
    if (fp == NULL) {
        printf("Open final_state.txt failed!\n");
        return -1;
    }

    for (std::map<std::string, struct RecordLine*>::iterator it = engine->table.begin(); it != engine->table.end(); ++it) {
        string cur_k = it->first;
        // cout<<"cur_k "<<cur_k<<endl;
        struct RecordLine* currl = it->second;
        // cout<<currl->records[0].createdXid<<" "<<currl->records[0].expiredXid<<" "<<currl->records[0].value<<endl;
        // cout<<currl->records[1].createdXid<<" "<<currl->records[1].expiredXid<<" "<<currl->records[1].value<<endl;
        if (currl->records[0].createdXid != -1) {
            fprintf(fp, "%s,%d\n", cur_k.c_str(), currl->records[0].value);
        } else if (currl->records[1].createdXid != -1) {
            fprintf(fp, "%s,%d\n", cur_k.c_str(), currl->records[1].value);
        } else continue;
    }

    fclose(fp);

    delete engine;

    return 0;
}