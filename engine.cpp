#include "headers.h"

extern bool x_is_active[MAX_TRANSACTION_NUM + 1];
extern vector<pair<string, char>> rollBackActions[MAX_TRANSACTION_NUM + 1];
long long getTime();

std::mutex copy_mtx;

bool Engine::recordIsVisible(struct Record* r, int xid) {
    if (r->createdXid == -1) return false;
    if (x_is_active[r->createdXid] && r->createdXid != xid) {
        return false;
    } else if (r->expiredXid != -1 && 
        (!x_is_active[r->expiredXid] || r->expiredXid == xid)) {
        return false;
    }
    
    return true;
}

bool Engine::recordIsLocked(struct Record* r, int xid) {
    return (r->expiredXid != -1 && x_is_active[r->expiredXid] && r->expiredXid != xid); 
}

int Engine::addRecord(string key, int value, int xid) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    RecordLine *cur;
    if (it == table.end()) {
        RecordLine *cur = new RecordLine();
        cur->records[0].createdXid = xid;
        cur->records[0].value = value;
        this->table.insert(std::pair<std::string, RecordLine*>(key, cur)); 
    } else {
        cur = it->second;
        if (cur->records[0].createdXid == -1 && cur->records[0].expiredXid == -1) {
            cur->records[0].createdXid = xid;
            cur->records[0].value = value;
        } else if (cur->records[1].createdXid == -1 && cur->records[1].expiredXid == -1) {
            cur->records[1].createdXid = xid;
            cur->records[1].value = value;
        }
        // Roll back handle?
    }

    // printf("in add op xid is %d , key is %s\n", xid, key.c_str());
    // printf("record0 %d %d %d record1 %d %d %d\n", cur->records[0].value, cur->records[0].createdXid, cur->records[0].expiredXid,
    // cur->records[1].value, cur->records[1].createdXid, cur->records[1].expiredXid);

    // if (cur->records[0].createdXid == xid && cur->records[1].createdXid == xid) {
    //     printf("in add op xid is %d , key is %s\n", xid, key.c_str());
    //     printf("shit record0 %d %d %d record1 %d %d %d\n", cur->records[0].value, cur->records[0].createdXid, cur->records[0].expiredXid,
    //     cur->records[1].value, cur->records[1].createdXid, cur->records[1].expiredXid);
    //     exit(-1);
    // }

    rollBackActions[xid].push_back(make_pair(key, 'A'));

    return 0;
}

int Engine::deleteRecord(string key, int xid, int& base) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    RecordLine *cur;
    if (it == table.end()) {
        // printf("No such record to delete!\n");
        return -1;
    } else {
        cur = it->second;
        cur->mtx.lock();
        if (recordIsLocked(&(cur->records[0]), xid) || recordIsLocked(&(cur->records[1]), xid)) {
            // printf("The record currently locked by another transaction!");
            cur->mtx.unlock();
            return -1;
        } else if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid == -1) {
            if (cur->records[1].expiredXid == xid ) {
                cur->records[0].createdXid = -1; // set in the same transaction before
                base = cur->records[0].value;
            } else {
                cur->records[0].expiredXid = xid;
                base = cur->records[0].value;
            }
        } else if (cur->records[1].createdXid != -1 && cur->records[1].expiredXid == -1) {
            if (cur->records[0].expiredXid == xid ) {
                cur->records[1].createdXid = -1; // set in the same transaction before
                base = cur->records[1].value;
            } else {
                cur->records[1].expiredXid = xid;
                base = cur->records[1].value;
            }
        }
        cur->mtx.unlock();
    }

    // printf("in delete op xid is %d , key is %s\n", xid, key.c_str());
    // printf("record0 %d %d %d record1 %d %d %d\n", cur->records[0].value, cur->records[0].createdXid, cur->records[0].expiredXid,
    // cur->records[1].value, cur->records[1].createdXid, cur->records[1].expiredXid);

    // if (cur->records[0].createdXid == xid && cur->records[1].createdXid == xid) {
    //     printf("in delete op xid is %d\n", xid);
    //     printf("shit record0 %d %d %d record1 %d %d %d\n", cur->records[0].value, cur->records[0].createdXid, cur->records[0].expiredXid,
    //     cur->records[1].value, cur->records[1].createdXid, cur->records[1].expiredXid);
    //     exit(-1);
    // }

    rollBackActions[xid].push_back(make_pair(key, 'D'));

    return 0;
}

int Engine::updateRecord(string key, int value, int xid) {
    this->mtx.lock();
    int base = 0;
    if (deleteRecord(key, xid, base) != 0) {
        this->mtx.unlock();
        return -1;
    } else {
        base = base + value;
        addRecord(key, base, xid);
        // Resource collect later?
    }
    this->mtx.unlock();
    return 0;
}

int Engine::getValue(string key, int xid, long long& ct) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    RecordLine *cur;
    if (it == table.end()) {
        // printf("No such record!\n");
        return INT_MIN;
    } else {
        cur = it->second;
        cur->mtx.lock();
        if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid == -1 && recordIsVisible(&(cur->records[0]), xid)) {
            cur->mtx.unlock();
            ct = getTime();
            return cur->records[0].value;
        } else if (cur->records[1].createdXid != -1 && cur->records[1].expiredXid == -1 && recordIsVisible(&(cur->records[1]), xid)) {
            cur->mtx.unlock();
            ct = getTime();
            return cur->records[1].value;
        } else if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid != -1 && recordIsVisible(&(cur->records[0]), xid)) {
            cur->mtx.unlock();
            ct = getTime();
            return cur->records[0].value;
        } else if (cur->records[1].createdXid != -1 && cur->records[1].expiredXid != -1 && recordIsVisible(&(cur->records[1]), xid)) {
            cur->mtx.unlock();
            ct = getTime();
            return cur->records[1].value;
        }
        cur->mtx.unlock();
    }

    // printf("record0 %d %d %d record1 %d %d %d\n", cur->records[0].value, cur->records[0].createdXid, cur->records[0].expiredXid,
    //     cur->records[1].value, cur->records[1].createdXid, cur->records[1].expiredXid);
    // printf("No such record!\n");
    return INT_MIN;
}

int Engine::reclaim(string key, int xid) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    if (it == table.end()) {
        // printf("No such record!\n");
        return -1;
    } else {
        RecordLine *cur = it->second;
        if (cur->records[0].expiredXid != -1 && cur->records[0].expiredXid == xid) {
            cur->records[0].createdXid = -1;
            cur->records[0].expiredXid = -1;
        }
        if (cur->records[1].expiredXid != -1 && cur->records[1].expiredXid == xid) {
            cur->records[1].createdXid = -1;
            cur->records[1].expiredXid = -1;
        }
    }

    return 0;
}

int Engine::doRollback(vector<pair<string, char>>& rbas, int xid) {
    for (int i = rbas.size() - 1; i>=0; --i) {
        string ckey = rbas[i].first;
        char ccmd = rbas[i].second;
        std::map<string, RecordLine *>::iterator it;
        it = this->table.find(ckey);
        if (it == table.end()) {
            printf("No such record!\n");
            return -1;
        } else {
            RecordLine *cur = it->second;
            cur->mtx.lock();
            if (recordIsLocked(&(cur->records[0]), xid) == false && recordIsLocked(&(cur->records[1]), xid) == false) {
                if (ccmd == 'A') {
                    if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid == -1 && cur->records[1].createdXid != -1 && cur->records[1].expiredXid != -1) {
                        cur->records[0].createdXid = -1;
                    } else if (cur->records[1].createdXid != -1 && cur->records[1].expiredXid == -1 && cur->records[0].createdXid != -1 && cur->records[0].expiredXid != -1) {
                        cur->records[1].createdXid = -1;
                    }
                } else if (ccmd == 'D') {
                    if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid != -1) {
                        cur->records[0].expiredXid = -1;
                    } else if (cur->records[1].createdXid != -1 && cur->records[1].expiredXid != -1) {
                        cur->records[1].expiredXid = -1;
                    }
                }
            }
            cur->mtx.unlock();
        }
    }

    return 0;
}

void Engine::copyData(std::map<std::string, int>& cur_table, int xid) {
    this->mtx.lock();
    for (std::map<std::string, struct RecordLine*>::iterator it = this->table.begin(); it != this->table.end(); ++it) {
        string cur_k = it->first;
        // cout<<"cur_k "<<cur_k<<endl;
        struct RecordLine* currl = it->second;
        // cout<<currl->records[0].createdXid<<" "<<currl->records[0].expiredXid<<" "<<currl->records[0].value<<endl;
        // cout<<currl->records[1].createdXid<<" "<<currl->records[1].expiredXid<<" "<<currl->records[1].value<<endl;
        if (this->recordIsVisible(&currl->records[0], xid)) {
            cur_table.insert(std::pair<std::string, int>(cur_k, currl->records[0].value));
        } else if (this->recordIsVisible(&currl->records[1], xid)) {
            cur_table.insert(std::pair<std::string, int>(cur_k, currl->records[1].value));
        } else {
            printf("shit\n");
            cout<<currl->records[0].createdXid<<" "<<currl->records[0].expiredXid<<" "<<currl->records[0].value<<" "<<x_is_active[currl->records[0].createdXid]<<endl;
            cout<<currl->records[1].createdXid<<" "<<currl->records[1].expiredXid<<" "<<currl->records[1].value<<" "<<x_is_active[currl->records[1].createdXid]<<endl;
            cout<<xid<<endl;
            exit(-1);
        }
    }
    this->mtx.unlock();
}

// void Engine::nsUpdate(std::map<std::string, int>& cur_table) {
//     this->mtx.lock();
//     for (std::map<std::string, int>::iterator it = cur_table.begin(); it != cur_table.end(); ++it) {
//         string cur_k = it->first;
//         // cout<<"cur_k "<<cur_k<<endl;
//         this->       
//     }
//     this->mtx.unlock();
// }