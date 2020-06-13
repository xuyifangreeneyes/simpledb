package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, PageLock> lockMap = new ConcurrentHashMap<>();
    private Map<TransactionId, Set<PageId>> holdingMap = new ConcurrentHashMap<>();
    private Map<TransactionId, Set<TransactionId>> waitingMap = new ConcurrentHashMap<>();

    public void requireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        lockMap.putIfAbsent(pid, new PageLock(pid));
        PageLock lock = lockMap.get(pid);
        synchronized (lock) {
            while (!lock.require(tid, perm)) {
                // check whether deadlock exists
                updateDependency(tid, pid);
                if (findDeadlock(tid)) {
                    updateDependency(tid, null);
                    throw new TransactionAbortedException();
                }

                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        updateDependency(tid, null);

        holdingMap.putIfAbsent(tid, new HashSet<>());
        holdingMap.get(tid).add(pid);
    }

    private synchronized void updateDependency(TransactionId tid, PageId pid) {
        waitingMap.putIfAbsent(tid, new HashSet<>());
        Set<TransactionId> waiting = waitingMap.get(tid);
        waiting.clear();
        if (pid != null) {
            waiting.addAll(lockMap.get(pid).getHolders());
        }
    }

    private synchronized boolean findDeadlock(TransactionId tid) {
        return dfs(tid, tid, new HashSet<>());
    }

    private boolean dfs(TransactionId cur, TransactionId tgt, Set<TransactionId> visited) {
        visited.add(cur);
        Set<TransactionId> waiting = waitingMap.get(cur);
        if (waiting == null) {
            return false;
        }
        for (TransactionId nxt : waiting) {
            if (nxt == tgt) {
                return true;
            }
            if (visited.contains(nxt)) {
                continue;
            }
            if (dfs(nxt, tgt, visited)) {
                return true;
            }
        }
        return false;
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        assert lockMap.containsKey(pid);
        lockMap.get(pid).release(tid);
        holdingMap.get(tid).remove(pid);
    }

    public void releaseLocks(TransactionId tid) {
        Set<PageId> lockedPids = holdingMap.get(tid);
        holdingMap.remove(tid);
        if (lockedPids != null) {
            for (PageId pid : lockedPids) {
                lockMap.get(pid).release(tid);
            }
        }
    }

    public Set<PageId> getHeldLocks(TransactionId tid) {
        return holdingMap.get(tid);
    }

    public boolean holdLock(TransactionId tid, PageId pid) {
        lockMap.putIfAbsent(pid, new PageLock(pid));
        return lockMap.get(pid).hold(tid);
    }



}
