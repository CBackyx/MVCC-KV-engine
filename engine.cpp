#include "headers.h"

extern bool x_is_active[MAX_TRANSACTION_NUM];

bool Engine::recordIsVisible(struct Record* r, int xid) {
    if (x_is_active[xid] && r->createdXid != xid) {
        return false;
    } else if (r->expiredXid != -1 && 
        (!x_is_active[r->expiredXid] || r->expiredXid == xid)) {
        return false;
    }
    
    return true;
}

bool Engine::recordIsLocked(struct Record* r) {
    return (r->expiredXid != -1 && x_is_active[r->expiredXid]);
}

int Engine::addRecord(string key, int value, int xid) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    if (it == table.end()) {
        RecordLine *cur = new RecordLine();
        cur->records[0].createdXid = xid;
        cur->records[0].value = value;
        this->table.insert(std::pair<std::string, RecordLine*>(key, cur)); 
    } else {
        RecordLine *cur = it->second;
        if (cur->records[0].createdXid == -1 && cur->records[0].expiredXid == -1) {
            cur->records[0].createdXid = xid;
            cur->records[0].value = value;
        } else {
            cur->records[1].createdXid = xid;
            cur->records[1].value = value;
        }
        // Roll back handle?
    }

    return 0;
}

int Engine::deleteRecord(string key, int xid) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    if (it == table.end()) {
        printf("No such record to delete!\n");
        return -1;
    } else {
        RecordLine *cur = it->second;
        if (recordIsLocked(&(cur->records[0])) || recordIsLocked(&(cur->records[1]))) {
            printf("The record currently locked by another transaction!");
            return -1;
        } else if (cur->records[0].createdXid != -1 && cur->records[0].expiredXid == -1) {
            cur->records[0].expiredXid = xid;
        } else {
            cur->records[1].expiredXid = xid;
        }
    }

    return 0;
}

int Engine::updateRecord(string key, int value, int xid) {
    if (deleteRecord(key, xid) != 0) {
        return -1;
    } else {
        addRecord(key, value, xid);
        // Resource collect later?
    }
    
    return 0;
}

int Engine::getValue(string key, int xid) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    if (it == table.end()) {
        printf("No such record!\n");
        return -1;
    } else {
        RecordLine *cur = it->second;
        if (cur->records[0].createdXid != -1 && !recordIsLocked(&(cur->records[0]))) {
            return cur->records[0].value;
        } else if (cur->records[1].createdXid != -1 && !recordIsLocked(&(cur->records[1]))) {
            return cur->records[1].value;
        }
    }

    printf("No such record!\n");
    return -1;
}

int Engine::reclaim(string key) {
    std::map<string, RecordLine *>::iterator it;
    it = this->table.find(key);
    if (it == table.end()) {
        printf("No such record!\n");
        return -1;
    } else {
        RecordLine *cur = it->second;
        if (cur->records[0].expiredXid != -1 && !x_is_active[cur->records[0].expiredXid]) {
            cur->records[0].createdXid = -1;
            cur->records[0].expiredXid = -1;
        }
        if (cur->records[1].expiredXid != -1 && !x_is_active[cur->records[1].expiredXid]) {
            cur->records[1].createdXid = -1;
            cur->records[1].expiredXid = -1;
        }
    }
     
    return 0;
}