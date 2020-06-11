package simpledb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, PageLock> pageLocks = new ConcurrentHashMap<>();


}
