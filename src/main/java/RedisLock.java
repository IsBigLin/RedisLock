import redis.clients.jedis.Jedis;

/**
 * Created by lnq on 2017/3/21.
 */

/**
 * 分布式锁可以基于很多种方式实现，比如zookeeper、redis...。不管哪种方式，
 * 他的基本原理是不变的：用一个状态值表示锁，对锁的占用和释放通过状态值来标识。
 *
 * 可以用基于redis的分布式锁
 * 参考博客：http://www.cnblogs.com/0201zcr/p/5942748.html
 *             http://lixiaohui.iteye.com/blog/2320554
 */
public class RedisLock {

    private Jedis jedis;

    private static final int DEFAULT_ACQUIRY_RESOLUTION_MILLIS = 100;
    /**
     * Lock key path.
     */
    private String lockKey;

    /**
     * 锁超时时间，防止线程在入锁以后，无限的执行等待
     */
    private int expireMsecs = 60 * 1000;

    /**
     * 锁等待时间，防止线程饥饿
     */
    private int timeoutMsecs = 10 * 1000;

    private volatile boolean locked = false;

    public RedisLock(Jedis jedis, String lockKey) {
        this.jedis = jedis;
        this.lockKey = lockKey + "_lock";
    }

    public RedisLock(Jedis jedis, String lockKey, int timeoutMsecs) {
        this(jedis, lockKey);
        this.timeoutMsecs = timeoutMsecs;
    }

    public RedisLock(Jedis jedis, String lockKey, int timeoutMsecs, int expireMsecs) {
        this(jedis, lockKey, timeoutMsecs);
        this.expireMsecs = expireMsecs;
    }

    public String getLockKey() {
        return lockKey;
    }

    private boolean setNX(final String key, final String value) {
        return jedis.setnx(key,value) == 1 ? false : true;
    }

    /**
     * 获得 lock.
     * 实现思路: 主要是使用了redis 的setnx命令,缓存了锁.
     * reids缓存的key是锁的key,所有的共享, value是锁的到期时间(注意:这里把过期时间放在value了,没有时间上设置其超时时间)
     * 执行过程:
     * 1.通过setnx尝试设置某个key的值,成功(当前没有这个锁)则返回,成功获得锁
     * 2.锁已经存在则获取锁的到期时间,和当前时间比较,超时的话,则设置新的值
     *
     * @return true if lock is acquired, false acquire timeouted
     * @throws InterruptedException in case of thread interruption
     */
    public boolean lock() throws InterruptedException {
        int timeout = timeoutMsecs;
        while (timeout >= 0) {
            long expires = System.currentTimeMillis() + expireMsecs + 1;
            String expiresStr = String.valueOf(expires); //锁到期时间
            if (this.setNX(lockKey, expiresStr)) {
                locked = true;
                return true;
            }

            String currentValueStr = jedis.get(lockKey); //redis里的时间
            if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) { //lock is expired，锁超时
                String oldValueStr = jedis.getSet(lockKey, expiresStr);
                //防止 多个线程同事访问，使用getSet方法重置了oldValueStr,oldValueStr每个线程是不一样的，因为getSet是原子性
                if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
                    // lock acquired
                    locked = true;
                    return true;
                }
            }
            timeout -= DEFAULT_ACQUIRY_RESOLUTION_MILLIS;

            /*
                延迟100 毫秒,  这里使用随机时间可能会好一点,可以防止饥饿进程的出现,即,当同时到达多个进程,
                只会有一个进程获得锁,其他的都用同样的频率进行尝试,后面有来了一些进行,也以同样的频率申请锁,这将可能导致前面来的锁得不到满足.
                使用随机的等待时间可以一定程度上保证公平性
             */
            Thread.sleep(DEFAULT_ACQUIRY_RESOLUTION_MILLIS);

        }
        return false;
    }

    /**
     * Acqurired lock release.
     */
    public synchronized void unlock() {
        if (locked) {
            jedis.del(lockKey);
            locked = false;
        }
    }

    public boolean isTimeout(){
        String currentValueStr = jedis.get(lockKey);
        return Long.parseLong(currentValueStr) < System.currentTimeMillis();
    }
}
