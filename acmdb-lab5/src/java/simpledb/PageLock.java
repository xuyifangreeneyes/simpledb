package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PageLock {
    private Set<TransactionId> shared = ConcurrentHashMap.newKeySet();
    private TransactionId exclusive = null;
    private PageId pid;

    public PageLock(PageId pid) {
        this.pid = pid;
    }

    public synchronized boolean require(TransactionId tid, Permissions perm) {
        if (perm == Permissions.READ_WRITE) {
            return requireExclusive(tid);
        } else {
            assert perm == Permissions.READ_ONLY;
            return requireShared(tid);
        }
    }

    private boolean requireShared(TransactionId tid) {
        if (exclusive == null) {
            shared.add(tid);
            return true;
        } else {
            return exclusive == tid;
        }
    }

    private boolean requireExclusive(TransactionId tid) {
        if (exclusive == null) {
            if (shared.isEmpty()) {
                exclusive = tid;
                return true;
            } else {
                if (shared.contains(tid) && shared.size() == 1) {
                    shared.remove(tid);
                    exclusive = tid;
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return exclusive == tid;
        }
    }

//    public synchronized boolean isExclusive(TransactionId tid) {
//        return exclusive == tid;
//    }

    public synchronized void release(TransactionId tid) {
        if (exclusive == null) {
            assert shared.contains(tid);
            shared.remove(tid);
        } else {
            assert exclusive == tid;
            exclusive = null;
        }
        notify();
    }

    public synchronized boolean hold(TransactionId tid) {
        if (exclusive == null) {
            return shared.contains(tid);
        } else {
            return exclusive == tid;
        }
    }

    public synchronized Set<TransactionId> getHolders() {
        Set<TransactionId> holders = new HashSet<>();
        if (exclusive == null) {
            holders.addAll(shared);
        } else {
            holders.add(exclusive);
        }
        return holders;
    }

}
