#include "headers.h"

using namespace std;
using namespace std::chrono;

string threadFiles[MAX_THREAD_NUM];
int threadIDs[MAX_THREAD_NUM];
thread ths[MAX_THREAD_NUM];
bool x_is_active[MAX_TRANSACTION_NUM + 1];
vector<pair<string, char>> rollBackActions[MAX_TRANSACTION_NUM + 1];

Engine *engine;

time_point<high_resolution_clock> start;

string attr_A = "attr_A";
string attr_B = "attr_B";
string attr_C = "attr_C";
string attr_D = "attr_D";

int doThread(int id);
long long getTime();

int doThread(int id) {
    int cur_v = 0;
    int cur_xid = 0;
    char cmd_buffer[MAX_COMMAND_SIZE];
    char tar[MAX_KEY_SIZE];
    char cur_op;
    int cur_op_num;
    string cur_k;

    // FILE *fp;
    ifstream fin(threadFiles[id].c_str());
    // fp = fopen(threadFiles[id].c_str(), "rb");
    // if (fp == NULL) {
    //     printf("Open input file for thread %d failed!\n", id);
    //     return -1;
    // }

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

    char xbuffer[MAX_TRANSACTION_SIZE][MAX_COMMAND_SIZE]; // for roll back
    char obuffer[MAX_TRANSACTION_SIZE][MAX_COMMAND_SIZE];
    int ocnt = 0;
    int xcnt = 0;
    int scnt = 0;

    while (fin.getline(cmd_buffer, MAX_COMMAND_SIZE)) {
        if (cmd_buffer[0] != 'C') {
            strcpy(xbuffer[xcnt], cmd_buffer);
            xcnt++;
            continue;
        }
        strcpy(xbuffer[xcnt], cmd_buffer);
        xcnt++;
        // printf("shit thread %d\n", id);
        while (true) {
            // printf("shit\n");
            // printf("%d\n", cur_xid);
            bool committed = false;
            bool needRedo = false;
            for (int i=0; i<xcnt; ++i) {
                // printf("%c\n", xbuffer[i][0]);
                // printf("%d\n", i);
                switch (xbuffer[i][0]) {
                    case 'B':
                        // Begin
                        sscanf(xbuffer[i] + 6, "%d", &cur_xid);
                        // sprintf(obuffer[ocnt++], "The begin of txn %d is:\n", cur_xid);
                        // for (std::map<std::string, struct RecordLine*>::iterator it = engine->table.begin(); it != engine->table.end(); ++it) {
                        //     string cur_k = it->first;
                        //     // cout<<"cur_k "<<cur_k<<endl;
                        //     struct RecordLine* currl = it->second;
                        //     // cout<<currl->records[0].createdXid<<" "<<currl->records[0].expiredXid<<" "<<currl->records[0].value<<endl;
                        //     // cout<<currl->records[1].createdXid<<" "<<currl->records[1].expiredXid<<" "<<currl->records[1].value<<endl;
                        //     sprintf(obuffer[ocnt++],  "key = %s record0 %d %d %d record1 %d %d %d\n", cur_k.c_str(), currl->records[0].value, currl->records[0].createdXid, currl->records[0].expiredXid,
                        //     currl->records[1].value, currl->records[1].createdXid, currl->records[1].expiredXid);
                        // }
                        // printf("cur_xid, %d\n", cur_xid);
                        x_is_active[cur_xid] = true;
                        sprintf(obuffer[ocnt++], "%d,BEGIN,%lld,", cur_xid, getTime());
                        // printf("Thread %d Begin transaction %d\n", id, cur_xid);
                        break;
                    case 'C':
                        // Commit
                        sscanf(xbuffer[i] + 7, "%d", &cur_xid);
                        sprintf(obuffer[ocnt++], "%d,END,%lld,", cur_xid, getTime());
                        // printf("Thread %d End transaction %d\n", id, cur_xid);
                        // flash the obuffer
                        for (int k=0; k<ocnt; ++k) {
                            fprintf(wfp, "%s\n", obuffer[k]);
                            fflush(wfp);
                        }
                        ocnt = 0;
                        // Reclaim the resources
                        for (auto x:toReclaim) {
                            engine->reclaim(x, cur_xid);
                        }
                        toReclaim.clear();
                        x_is_active[cur_xid] = false;
                        committed = true;
                        break;
                    case 'R':
                        // Read
                        sscanf(xbuffer[i] + 5, "%s", tar);
                        cur_k = tar;
                        cur_v = engine->getValue(cur_k, cur_xid);
                        scnt = 0;
                        while (cur_v == INT_MIN) {
                            // printf("Read failed in txn %d, %s!\n", cur_xid, tar);
                            std::this_thread::sleep_for(std::chrono::microseconds(rand()%10 + 1));
                            cur_v = engine->getValue(cur_k, cur_xid);
                            if (++scnt > 5) {
                                needRedo = true;
                                break;
                            } 
                        }
                        sprintf(obuffer[ocnt++], "%d,%s,%lld,%d", cur_xid, tar, getTime(), cur_v);
                        // printf("Thread %d Read %s %d\n", id, tar, cur_v);
                        break;
                    case 'S':
                        // Set
                        for (int k=0; k<100; k++) {
                            if (xbuffer[i][k] == ',') {
                                sscanf(xbuffer[i] + k + 2, "%s %c %d", tar, &cur_op, &cur_op_num);
                                break;
                            }
                        }
                        // printf("%s, ++ %s ++ %c ++ %d\n", tar, tar, cur_op, cur_op_num);
                        cur_k = tar;
                        // cur_v = engine->getValue(cur_k, cur_xid);
                        // scnt = 0;
                        // while (cur_v == INT_MIN) {
                        //     // printf("Read failed when updating!\n");
                        //     std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10 + 1));
                        //     cur_v = engine->getValue(cur_k, cur_xid);
                        //     if (++scnt > 5) {
                        //         needRedo = true;
                        //         break;
                        //     }
                        // }
                        // if (needRedo) break;
                        // if (cur_op == '+') cur_v += cur_op_num;
                        // else cur_v -= cur_op_num;

                        if (cur_op == '+') cur_op_num = cur_op_num;
                        else cur_op_num = -cur_op_num;

                        scnt = 0;
                        while (engine->updateRecord(cur_k, cur_op_num, cur_xid) != 0) {
                            // printf("Update blocked!\n");
                            // printf("transaction %d blocked\n", cur_xid);
                            // printf("%s %c %d \n", tar, cur_op, cur_op_num);
                            std::this_thread::sleep_for(std::chrono::microseconds(rand() % 10 + 1));
                            if (++scnt > 5) {
                                needRedo = true;
                                break;
                            } 
                        }
                        // printf("Thread %d Set %s %d\n", id, tar, cur_v);
                        if (!needRedo) toReclaim.push_back(cur_k);
                        // sprintf(obuffer[ocnt++], "SET %d,%s,%lld,%d", cur_xid, tar, getTime(), cur_v);
                        break;
                    default:
                        printf("Unrecognized command!\n");
                        exit(-1);
                }
                if (needRedo) {
                    // printf("halo\n");
                    break;
                }
                if (committed) {
                    break;
                }
            }
            if (needRedo) { 
                // printf("redo %d\n", cur_xid);              
                // rollback
                // fprintf(wfp, "redo txn %d\n", cur_xid);
                // fflush(wfp);
                engine->doRollback(rollBackActions[cur_xid], cur_xid);
                // redo
                rollBackActions[cur_xid].clear();
                ocnt = 0;
                toReclaim.clear();
                x_is_active[cur_xid] = false;
                std::this_thread::sleep_for (std::chrono::microseconds(rand()%1000 + 1));
                x_is_active[cur_xid] = true;
                continue;
            } else {
                // fprintf(wfp, "The end of txn %d is:\n", cur_xid);
                // for (std::map<std::string, struct RecordLine*>::iterator it = engine->table.begin(); it != engine->table.end(); ++it) {
                //     string cur_k = it->first;
                //     // cout<<"cur_k "<<cur_k<<endl;
                //     struct RecordLine* currl = it->second;
                //     // cout<<currl->records[0].createdXid<<" "<<currl->records[0].expiredXid<<" "<<currl->records[0].value<<endl;
                //     // cout<<currl->records[1].createdXid<<" "<<currl->records[1].expiredXid<<" "<<currl->records[1].value<<endl;
                //     fprintf(wfp,  "key = %s record0 %d %d %d record1 %d %d %d\n", cur_k.c_str(), currl->records[0].value, currl->records[0].createdXid, currl->records[0].expiredXid,
                //     currl->records[1].value, currl->records[1].createdXid, currl->records[1].expiredXid);
                //     if (engine->recordIsVisible(&(currl->records[0]), cur_xid)) {
                //         fprintf(wfp, "%s,%d\n", cur_k.c_str(), currl->records[0].value);
                //     } else if (engine->recordIsVisible(&(currl->records[1]), cur_xid)) {
                //         fprintf(wfp, "%s,%d\n", cur_k.c_str(), currl->records[1].value);
                //     } else continue;
                // }
                fflush(wfp);
                rollBackActions[cur_xid].clear();
                xcnt = 0;
                ocnt = 0;
                toReclaim.clear();
                break;
            }
        }
    }
    
    fin.close();
    // fclose(fp);
    fclose(wfp);
    return 0;
}

// Actually this is a duration since start point
long long getTime() {
    time_point<high_resolution_clock> end;
    double duration;
    end = high_resolution_clock::now();//获取当前时间
	auto dur = duration_cast<nanoseconds>(end - start);
	duration = double(dur.count()) * nanoseconds::period::num / nanoseconds::period::den * 1000000000;
    return (long long)duration;
}

int main() {
    // Initialize the KV engine
    engine = new Engine();

    srand(1);

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

    printf("Total time cost is %llf\n", (double)getTime()/1000000000);

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