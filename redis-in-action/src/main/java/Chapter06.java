import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @Date: 2024/7/10 15:44
 * @Description: FakeGame公司的游戏中聊天社交功能、工会邮件发送、游戏商品交易、不登入游戏情况下进行商品买卖、分析大体积日志文件。
 * FakeGarbage公司打算开发一个移动应用程序接收短信彩信。
 * <p></p>
 * 1. 构建两个前缀匹配自动补全程序<br>
 * 2. 通过构建分布式锁来提高性能<br>
 * 3. 通过开发计数信号量来控制并发<br>
 * 4. 构建两个不同用途的任务队列<br>
 * 5. 通过消息拉取系统来实现延迟消息传递<br>
 * 6. 如何进行文件分发<br>
 */

public class Chapter06 {
    public static final void main(String[] args)
            throws Exception {
        new Chapter06().run();
    }

    public void run()
            throws InterruptedException, IOException {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testAddUpdateContact(conn);
        testAddressBookAutocomplete(conn);
        testDistributedLocking(conn);
        testCountingSemaphore(conn);
        testDelayedTasks(conn);
        testMultiRecipientMessaging(conn);
        testFileDistribution(conn);
    }

    public void testAddUpdateContact(Jedis conn) {
        System.out.println("\n----- testAddUpdateContact -----");
        conn.del("recent:user");

        System.out.println("Let's add a few contacts...");
        for (int i = 0; i < 10; i++) {
            addUpdateContact(conn, "user", "contact-" + ((int) Math.floor(i / 3)) + '-' + i);
        }
        System.out.println("Current recently contacted contacts");
        List<String> contacts = conn.lrange("recent:user", 0, -1);
        for (String contact : contacts) {
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 10;
        System.out.println();

        System.out.println("Let's pull one of the older ones up to the front");
        addUpdateContact(conn, "user", "contact-1-4");
        contacts = conn.lrange("recent:user", 0, 2);
        System.out.println("New top-3 contacts:");
        for (String contact : contacts) {
            System.out.println("  " + contact);
        }
        assert "contact-1-4".equals(contacts.get(0));
        System.out.println();

        System.out.println("Let's remove a contact...");
        removeContact(conn, "user", "contact-2-6");
        contacts = conn.lrange("recent:user", 0, -1);
        System.out.println("New contacts:");
        for (String contact : contacts) {
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 9;
        System.out.println();

        System.out.println("And let's finally autocomplete on ");
        List<String> all = conn.lrange("recent:user", 0, -1);
        contacts = fetchAutocompleteList(conn, "user", "c");
        assert all.equals(contacts);
        List<String> equiv = new ArrayList<String>();
        for (String contact : all) {
            if (contact.startsWith("contact-2-")) {
                equiv.add(contact);
            }
        }
        contacts = fetchAutocompleteList(conn, "user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contacts);
        assert equiv.equals(contacts);
        conn.del("recent:user");
    }

    public void testAddressBookAutocomplete(Jedis conn) {
        System.out.println("\n----- testAddressBookAutocomplete -----");
        conn.del("members:test");
        System.out.println("the start/end range of 'abc' is: " +
                Arrays.toString(findPrefixRange("abc")));
        System.out.println();

        System.out.println("Let's add a few people to the guild");
        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}) {
            joinGuild(conn, "test", name);
        }
        System.out.println();
        System.out.println("now let's try to find users with names starting with 'je':");
        Set<String> r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 3;

        System.out.println("jeff just left to join a different guild...");
        leaveGuild(conn, "test", "jeff");
        r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 2;
        conn.del("members:test");
    }

    public void testDistributedLocking(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testDistributedLocking -----");
        conn.del("lock:testlock");
        System.out.println("Getting an initial lock...");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("Got it!");
        System.out.println("Trying to get it again without releasing the first one...");
        assert acquireLockWithTimeout(conn, "testlock", 10, 1000) == null;
        System.out.println("Failed to get it!");
        System.out.println();

        System.out.println("Waiting for the lock to timeout...");
        Thread.sleep(2000);
        System.out.println("Getting the lock again...");
        String lockId = acquireLockWithTimeout(conn, "testlock", 1000, 1000);
        assert lockId != null;
        System.out.println("Got it!");
        System.out.println("Releasing the lock...");
        assert releaseLock(conn, "testlock", lockId);
        System.out.println("Released it...");
        System.out.println();

        System.out.println("Acquiring it again...");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("Got it!");
        conn.del("lock:testlock");
    }

    public void testCountingSemaphore(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testCountingSemaphore -----");
        conn.del("testsem", "testsem:owner", "testsem:counter");
        System.out.println("Getting 3 initial semaphores with a limit of 3...");
        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }
        System.out.println("Done!");
        System.out.println("Getting one more that should fail...");
        assert acquireFairSemaphore(conn, "testsem", 3, 1000) == null;
        System.out.println("Couldn't get it!");
        System.out.println();

        System.out.println("Lets's wait for some of them to time out");
        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = acquireFairSemaphore(conn, "testsem", 3, 1000);
        assert id != null;
        System.out.println("Got one!");
        System.out.println("Let's release it...");
        assert releaseFairSemaphore(conn, "testsem", id);
        System.out.println("Released!");
        System.out.println();
        System.out.println("And let's make sure we can get 3 more!");
        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }
        System.out.println("We got them!");
        conn.del("testsem", "testsem:owner", "testsem:counter");
    }

    public void testDelayedTasks(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testDelayedTasks -----");
        conn.del("queue:tqueue", "delayed:");
        System.out.println("Let's start some regular and delayed tasks...");
        for (long delay : new long[]{0, 500, 0, 1500}) {
            assert executeLater(conn, "tqueue", "testfn", new ArrayList<String>(), delay) != null;
        }
        long r = conn.llen("queue:tqueue");
        System.out.println("How many non-delayed tasks are there (should be 2)? " + r);
        assert r == 2;
        System.out.println();

        System.out.println("Let's start up a thread to bring those delayed tasks back...");
        PollQueueThread thread = new PollQueueThread();
        thread.start();
        System.out.println("Started.");
        System.out.println("Let's wait for those tasks to be prepared...");
        Thread.sleep(2000);
        thread.quit();
        thread.join();
        r = conn.llen("queue:tqueue");
        System.out.println("Waiting is over, how many tasks do we have (should be 4)? " + r);
        assert r == 4;
        conn.del("queue:tqueue", "delayed:");
    }

    public void testMultiRecipientMessaging(Jedis conn) {
        System.out.println("\n----- testMultiRecipientMessaging -----");
        conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");

        System.out.println("Let's create a new chat session with some recipients...");
        Set<String> recipients = new HashSet<String>();
        recipients.add("jeff");
        recipients.add("jenny");
        String chatId = createChat(conn, "joe", recipients, "message 1");
        System.out.println("Now let's send a few messages...");
        for (int i = 2; i < 5; i++) {
            sendMessage(conn, chatId, "joe", "message " + i);
        }
        System.out.println();

        System.out.println("And let's get the messages that are waiting for jeff and jenny...");
        List<ChatMessages> r1 = fetchPendingMessages(conn, "jeff");
        List<ChatMessages> r2 = fetchPendingMessages(conn, "jenny");
        System.out.println("They are the same? " + r1.equals(r2));
        assert r1.equals(r2);
        System.out.println("Those messages are:");
        for (ChatMessages chat : r1) {
            System.out.println("  chatId: " + chat.chatId);
            System.out.println("    messages:");
            for (Map<String, Object> message : chat.messages) {
                System.out.println("      " + message);
            }
        }

        conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");
    }

    public void testFileDistribution(Jedis conn)
            throws InterruptedException, IOException {
        System.out.println("\n----- testFileDistribution -----");
        String[] keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0) {
            conn.del(keys);
        }
        conn.del(
                "msgs:test:",
                "seen:0",
                "seen:source",
                "ids:test:",
                "chat:test:");

        System.out.println("Creating some temporary 'log' files...");
        File f1 = File.createTempFile("temp_redis_1_", ".txt");
        f1.deleteOnExit();
        Writer writer = new FileWriter(f1);
        writer.write("one line\n");
        writer.close();

        File f2 = File.createTempFile("temp_redis_2_", ".txt");
        f2.deleteOnExit();
        writer = new FileWriter(f2);
        for (int i = 0; i < 100; i++) {
            writer.write("many lines " + i + '\n');
        }
        writer.close();

        File f3 = File.createTempFile("temp_redis_3_", ".txt.gz");
        f3.deleteOnExit();
        writer = new OutputStreamWriter(
                new GZIPOutputStream(
                        new FileOutputStream(f3)));
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            writer.write("random line " + Long.toHexString(random.nextLong()) + '\n');
        }
        writer.close();

        long size = f3.length();
        System.out.println("Done.");
        System.out.println();
        System.out.println("Starting up a thread to copy logs to redis...");
        File path = f1.getParentFile();
        CopyLogsThread thread = new CopyLogsThread(path, "test:", 1, size);
        thread.start();

        System.out.println("Let's pause to let some logs get copied to Redis...");
        Thread.sleep(250);
        System.out.println();
        System.out.println("Okay, the logs should be ready. Let's process them!");

        System.out.println("Files should have 1, 100, and 1000 lines");
        TestCallback callback = new TestCallback();
        processLogsFromRedis(conn, "0", callback);
        System.out.println(Arrays.toString(callback.counts.toArray(new Integer[0])));
        assert callback.counts.get(0) == 1;
        assert callback.counts.get(1) == 100;
        assert callback.counts.get(2) == 1000;

        System.out.println();
        System.out.println("Let's wait for the copy thread to finish cleaning up...");
        thread.join();
        System.out.println("Done cleaning out Redis!");

        keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0) {
            conn.del(keys);
        }
        conn.del(
                "msgs:test:",
                "seen:0",
                "seen:source",
                "ids:test:",
                "chat:test:");
    }

    public class TestCallback
            implements Callback {
        private int index;
        public List<Integer> counts = new ArrayList<Integer>();

        public void callback(String line) {
            if (line == null) {
                index++;
                return;
            }
            while (counts.size() == index) {
                counts.add(0);
            }
            counts.set(index, counts.get(index) + 1);
        }
    }

    /**
     * 添加/更新一个联系人
     * 1. 指定联系人是否存在于最近联系人列表里，
     * 2. 将指定联系人添加至最近联系人最前，
     * 3. 修剪列表只保留100人。
     *
     * @param conn
     * @param user
     * @param contact
     */
    public void addUpdateContact(Jedis conn, String user, String contact) {
        String acList = "recent:" + user;
        Transaction trans = conn.multi();
        trans.lrem(acList, 0, contact);
        trans.lpush(acList, contact);
        trans.ltrim(acList, 0, 99);
        trans.exec();
    }

    /**
     * 移除指定联系人
     *
     * @param conn
     * @param user
     * @param contact
     */
    public void removeContact(Jedis conn, String user, String contact) {
        conn.lrem("recent:" + user, 0, contact);
    }

    /**
     * 获取自动补全列表并查找匹配用户，
     *
     * @param conn
     * @param user
     * @param prefix
     * @return
     */
    public List<String> fetchAutocompleteList(Jedis conn, String user, String prefix) {
        // 获取自动补全列表
        List<String> candidates = conn.lrange("recent:" + user, 0, -1);
        // 检查每个候选联系人
        List<String> matches = new ArrayList<String>();
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix)) {
                matches.add(candidate);
            }
        }
        return matches;
    }

    /**
     * 字符列表
     */
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";

    /**
     * 确认需要查找的范围：
     * 使用bisect模块在预先排好序的字符序列里找打前缀的最后一个字符，
     * 并据此来查找第一个排在该字符前面的字符。
     * <p></p>
     * 原理：ASCII编码
     * -eg: "abc{" 这个元素既位于abc之前，又位于带有abc前缀的合法名字之后
     *
     * @param prefix
     * @return
     */
    public String[] findPrefixRange(String prefix) {
        // 查找前缀字符所处位置
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        // 找到前驱字符
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        // 返回范围
        return new String[]{start, end};
    }

    /**
     * 加入工会
     *
     * @param conn
     * @param guild
     * @param user
     */
    public void joinGuild(Jedis conn, String guild, String user) {
        conn.zadd("members:" + guild, 0, user);
    }

    /**
     * 离开工会
     *
     * @param conn
     * @param guild
     * @param user
     */
    public void leaveGuild(Jedis conn, String guild, String user) {
        conn.zrem("members:" + guild, user);
    }

    /**
     * 自动补全函数
     * 1. 获取起始元素和结束元素，并添加到对应工会的自动补全有序集合，
     * 2. 根据起始和结束在有序的排名取出元素，
     * 3. 执行相应的清理工作。
     *
     * @param conn
     * @param guild
     * @param prefix
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set<String> autocompleteOnPrefix(Jedis conn, String guild, String prefix) {
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        // UUID加到起始和结束元素后面以防发生重复错误
        String identifier = UUID.randomUUID().toString();
        start += identifier;
        end += identifier;
        String zsetName = "members:" + guild;

        // 起始和结束元素添加到有序集合
        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true) {
            conn.watch(zsetName);
            // 找到两个被插入元素在有序集合中的排名
            int sindex = conn.zrank(zsetName, start).intValue();
            int eindex = conn.zrank(zsetName, end).intValue();
            int erange = Math.min(sindex + 9, eindex - 2);

            // 获取范围内的值
            Transaction trans = conn.multi();
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            trans.zrange(zsetName, sindex, erange);
            List<Object> results = trans.exec();
            if (results != null) {
                items = (Set<String>) results.get(results.size() - 1);
                break;
            }
        }

        // 如果有其他自动补全操作正在执行，那么从获取到的元素里面移除起始元素和结束元素
        for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ) {
            if (iterator.next().indexOf('{') != -1) {
                iterator.remove();
            }
        }
        return items;
    }

    public String acquireLock(Jedis conn, String lockName) {
        return acquireLock(conn, lockName, 10000);
    }

    /**
     * 基本加锁功能
     * - 使用SETNX命令，尝试在代表锁的键不存在的情况下为键设置一个值一次获取锁，
     * - 获取锁失败的时候会在给定的时限内进行重试直到成功获取锁或超时。
     *
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @return
     */
    public String acquireLock(Jedis conn, String lockName, long acquireTimeout) {
        // 128位随机标识符
        String identifier = UUID.randomUUID().toString();

        long end = System.currentTimeMillis() + acquireTimeout;
        // 尝试获取锁
        while (System.currentTimeMillis() < end) {
            if (conn.setnx("lock:" + lockName, identifier) == 1) {
                return identifier;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        return null;
    }

    /**
     * 带有超时限制特性的锁
     * - 保证锁在客户端已经崩溃的情况下仍能自动释放
     *
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    public String acquireLockWithTimeout(
            Jedis conn, String lockName, long acquireTimeout, long lockTimeout) {
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockName;
        // 过期时间
        int lockExpire = (int) (lockTimeout / 1000);

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, identifier) == 1) {
                conn.expire(lockKey, lockExpire);
                return identifier;
            }
            if (conn.ttl(lockKey) == -1) {
                conn.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        // null indicates that the lock was not acquired
        return null;
    }

    /**
     * 释放锁
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    public boolean releaseLock(Jedis conn, String lockName, String identifier) {
        String lockKey = "lock:" + lockName;

        while (true) {
            conn.watch(lockKey);
            if (identifier.equals(conn.get(lockKey))) {
                // 释放锁
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> results = trans.exec();
                if (results == null) {
                    continue;
                }
                return true;
            }

            conn.unwatch();
            break;
        }

        // 失去锁
        return false;
    }

    /**
     * 公平信号获取量操作
     *  - 从超时有序集合里移除过期元素，
     *  - 对超时有序集合和信号量拥有者有序集合执行交集运算，结果覆盖到信号量拥有者有序集合，
     *  - 计数器自增，将计数器生成的值添加到信号量拥有者有序集合；同时当前系统时间添加到超时有序集合。
     *  - 检查排名。
     *  <p></p>
     *  数据结构：
     *  1. 有序集合--信号量拥有者
     *      semaphoreRemoteOwner{"UUID":"counter", ...}
     *  2. 有序集合--存储超时信息
     *      semaphoreRemote{"UUID":"timestamp", ...}
     *  3. 字符串--计数器
     *      semaphoreRemoteCounter{"counter"}
     * @param conn
     * @param semname
     * @param limit
     * @param timeout
     * @return
     */
    public String acquireFairSemaphore(
            Jedis conn, String semname, int limit, long timeout) {
        // 128位随机标识符
        String identifier = UUID.randomUUID().toString();
        String czset = semname + ":owner";
        String ctr = semname + ":counter";

        // 删除超时信号量
        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        trans.zremrangeByScore(
                semname.getBytes(),
                "-inf".getBytes(),
                String.valueOf(now - timeout).getBytes());
        ZParams params = new ZParams();
        params.weights(1, 0);
        trans.zinterstore(czset, params, czset, semname);
        trans.incr(ctr);
        List<Object> results = trans.exec();
        int counter = ((Long) results.get(results.size() - 1)).intValue();

        // 尝试获取信号量
        trans = conn.multi();
        trans.zadd(semname, now, identifier);
        trans.zadd(czset, counter, identifier);
        trans.zrank(czset, identifier);
        results = trans.exec();
        // 检查排名判断是否获取了信号量
        int result = ((Long) results.get(results.size() - 1)).intValue();
        if (result < limit) {
            return identifier;
        }

        // 客户端未获取信号量，清理无用数据
        trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(czset, identifier);
        trans.exec();
        return null;
    }

    /**
     * 释放公平信号量
     *  - 需要同时从信号量拥有者以及超时有序集合里删除当前客户端标识符
     * @param conn
     * @param semname
     * @param identifier
     * @return
     */
    public boolean releaseFairSemaphore(
            Jedis conn, String semname, String identifier) {
        Transaction trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(semname + ":owner", identifier);
        List<Object> results = trans.exec();
        // true信号量已被正确释放，false表示释放的信号量已经超时而被删除了
        return (Long) results.get(results.size() - 1) == 1;
    }

    /**
     * 创建延时任务
     * <p></p>
     * 数据结构：
     *  1. 有序集合--延迟任务队列，每个被延迟的任务都是一个JSON列表
     *      delayed{JSON["identifier","queue","name","args"]:"timestamp", ...}
     * @param conn
     * @param queue
     * @param name
     * @param args
     * @param delay
     * @return
     */
    public String executeLater(
            Jedis conn, String queue, String name, List<String> args, long delay) {
        Gson gson = new Gson();
        // 唯一标识符
        String identifier = UUID.randomUUID().toString();
        // 准备入队任务
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        if (delay > 0) {
            // 延迟执行
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
            // 立即执行
            conn.rpush("queue:" + queue, item);
        }
        return identifier;
    }

    /**
     * 创建新群组
     * @param conn
     * @param sender
     * @param recipients
     * @param message
     * @return
     */
    public String createChat(Jedis conn, String sender, Set<String> recipients, String message) {
        // 新群组ID
        String chatId = String.valueOf(conn.incr("ids:chat:"));
        return createChat(conn, sender, recipients, message, chatId);
    }

    /**
     * 创建新群组聊天会话
     * <p></p>
     * 数据结构：
     *  1. 有序集合--群组聊天产生的内容
     *      chatID{"user":"messageID", ...}
     *  2. 有序集合--已读消息
     *      seenUser{"chatID", "maxMsgID", ...}
     * @param conn
     * @param sender
     * @param recipients
     * @param message
     * @param chatId
     * @return
     */
    public String createChat(
            Jedis conn, String sender, Set<String> recipients, String message, String chatId) {
        recipients.add(sender);

        Transaction trans = conn.multi();
        for (String recipient : recipients) {
            trans.zadd("chat:" + chatId, 0, recipient);
            trans.zadd("seen:" + recipient, 0, chatId);
        }
        trans.exec();

        return sendMessage(conn, chatId, sender, message);
    }

    /**
     * 使用锁实现发送消息
     * @param conn
     * @param chatId
     * @param sender
     * @param message
     * @return
     */
    public String sendMessage(Jedis conn, String chatId, String sender, String message) {
        String identifier = acquireLock(conn, "chat:" + chatId);
        if (identifier == null) {
            throw new RuntimeException("Couldn't get the lock");
        }
        try {
            // 准备消息各项信息JSON
            long messageId = conn.incr("ids:" + chatId);
            HashMap<String, Object> values = new HashMap<String, Object>();
            values.put("id", messageId);
            values.put("ts", System.currentTimeMillis());
            values.put("sender", sender);
            values.put("message", message);
            String packed = new Gson().toJson(values);

            // 发送消息至群组
            conn.zadd("msgs:" + chatId, messageId, packed);
        } finally {
            releaseLock(conn, "chat:" + chatId, identifier);
        }
        return chatId;
    }

    /**
     * 获取消息操作
     *  - 对记录用户的有序集合执行ZRANGE命令以获取群组ID和已读消息ID，
     *  - 对用户参与的所有群组的消息有序集合执行ZRANGEBYSCORE命令以获取用户在个群组内未读消息，
     *  - 获取聊天消息后对相应集合更新。
     * @param conn
     * @param recipient
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<ChatMessages> fetchPendingMessages(Jedis conn, String recipient) {
        // 获取最后接到消息的ID
        Set<Tuple> seenSet = conn.zrangeWithScores("seen:" + recipient, 0, -1);
        List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

        // 获取所有未读消息
        Transaction trans = conn.multi();
        for (Tuple tuple : seenList) {
            String chatId = tuple.getElement();
            int seenId = (int) tuple.getScore();
            trans.zrangeByScore("msgs:" + chatId, String.valueOf(seenId + 1), "inf");
        }
        List<Object> results = trans.exec();

        Gson gson = new Gson();
        Iterator<Tuple> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = results.iterator();

        List<ChatMessages> chatMessages = new ArrayList<ChatMessages>();
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        while (seenIterator.hasNext()) {
            Tuple seen = seenIterator.next();
            Set<String> messageStrings = (Set<String>) resultsIterator.next();
            if (messageStrings.size() == 0) {
                continue;
            }

            // 使用最新收到的消息更新群组有序集合
            int seenId = 0;
            String chatId = seen.getElement();
            List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
            for (String messageJson : messageStrings) {
                Map<String, Object> message = (Map<String, Object>) gson.fromJson(
                        messageJson, new TypeToken<Map<String, Object>>() {
                        }.getType());
                int messageId = ((Double) message.get("id")).intValue();
                if (messageId > seenId) {
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }

            // 更新已读消息有序集合
            conn.zadd("chat:" + chatId, seenId, recipient);
            seenUpdates.add(new Object[]{"seen:" + recipient, seenId, chatId});

            Set<Tuple> minIdSet = conn.zrangeWithScores("chat:" + chatId, 0, 0);
            if (minIdSet.size() > 0) {
                msgRemoves.add(new Object[]{
                        "msgs:" + chatId, minIdSet.iterator().next().getScore()});
            }
            chatMessages.add(new ChatMessages(chatId, messages));
        }

        // 清除已经被所有人阅读过的消息
        trans = conn.multi();
        for (Object[] seenUpdate : seenUpdates) {
            trans.zadd(
                    (String) seenUpdate[0],
                    (Integer) seenUpdate[1],
                    (String) seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves) {
            trans.zremrangeByScore(
                    (String) msgRemove[0], 0, ((Double) msgRemove[1]).intValue());
        }
        trans.exec();

        return chatMessages;
    }

    /**
     * 处理日志文件
     *  - 从群组里获取日志文件名，根据名字对Redis里日志文件处理，
     *  - 处理完成后对复制进程正在等待的键进行更新，
     *  - 使用回调函数处理每个日志行并更新聚合数据。
     *
     * @param conn
     * @param id
     * @param callback
     * @throws InterruptedException
     * @throws IOException
     */
    public void processLogsFromRedis(Jedis conn, String id, Callback callback)
            throws InterruptedException, IOException {
        while (true) {
            // 获取文件列表
            List<ChatMessages> fdata = fetchPendingMessages(conn, id);

            for (ChatMessages messages : fdata) {
                for (Map<String, Object> message : messages.messages) {
                    String logFile = (String) message.get("message");

                    if (":done".equals(logFile)) {
                        return;
                    }
                    if (logFile == null || logFile.length() == 0) {
                        continue;
                    }

                    // 选择块读取器block reader
                    InputStream in = new RedisInputStream(
                            conn, messages.chatId + logFile);
                    if (logFile.endsWith(".gz")) {
                        in = new GZIPInputStream(in);
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        String line = null;
                        // 遍历日志行
                        while ((line = reader.readLine()) != null) {
                            // 将日志传递给回调函数
                            callback.callback(line);
                        }
                        // 强制刷新聚合数据
                        callback.callback(null);
                    } finally {
                        reader.close();
                    }

                    // 向文件发送者汇报日志处理完毕
                    conn.incr(messages.chatId + logFile + ":done");
                }
            }

            if (fdata.size() == 0) {
                Thread.sleep(100);
            }
        }
    }

    public class RedisInputStream
            extends InputStream {
        private Jedis conn;
        private String key;
        private int pos;

        public RedisInputStream(Jedis conn, String key) {
            this.conn = conn;
            this.key = key;
        }

        @Override
        public int available()
                throws IOException {
            long len = conn.strlen(key);
            return (int) (len - pos);
        }

        @Override
        public int read()
                throws IOException {
            byte[] block = conn.substr(key.getBytes(), pos, pos);
            if (block == null || block.length == 0) {
                return -1;
            }
            pos++;
            return (int) (block[0] & 0xff);
        }

        @Override
        public int read(byte[] buf, int off, int len)
                throws IOException {
            byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0) {
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public interface Callback {
        void callback(String line);
    }

    public class ChatMessages {
        public String chatId;
        public List<Map<String, Object>> messages;

        public ChatMessages(String chatId, List<Map<String, Object>> messages) {
            this.chatId = chatId;
            this.messages = messages;
        }

        public boolean equals(Object other) {
            if (!(other instanceof ChatMessages)) {
                return false;
            }
            ChatMessages otherCm = (ChatMessages) other;
            return chatId.equals(otherCm.chatId) &&
                    messages.equals(otherCm.messages);
        }
    }

    /**
     * 从延迟队列里获取可执行任务
     */
    public class PollQueueThread
            extends Thread {
        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread() {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                // 获取队列中的第一个任务
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try {
                        sleep(10);
                    } catch (InterruptedException ie) {
                        Thread.interrupted();
                    }
                    continue;
                }

                // 解码，确定任务应该被推入哪个队列
                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];

                // 尝试获取锁以对任务进行移动
                String locked = acquireLock(conn, identifier);
                if (locked == null) {
                    continue;
                }

                // 推入相应任务队列
                if (conn.zrem("delayed:", json) == 1) {
                    conn.rpush("queue:" + queue, json);
                }

                releaseLock(conn, identifier, locked);
            }
        }
    }

    /**
     * 复制日志文件并在之后对无用数据进行清理
     */
    public class CopyLogsThread
            extends Thread {
        private Jedis conn;
        private File path;
        private String channel;
        private int count;
        private long limit;

        public CopyLogsThread(File path, String channel, int count, long limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.path = path;
            this.channel = channel;
            this.count = count;
            this.limit = limit;
        }

        public void run() {
            Deque<File> waiting = new ArrayDeque<File>();
            long bytesInRedis = 0;

            Set<String> recipients = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                recipients.add(String.valueOf(i));
            }
            createChat(conn, "source", recipients, "", channel);
            File[] logFiles = path.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.startsWith("temp_redis");
                }
            });
            Arrays.sort(logFiles);
            // 遍历所有日志文件
            for (File logFile : logFiles) {
                long fsize = logFile.length();
                // 若程序需要更多的空间则清理已处理的文件
                while ((bytesInRedis + fsize) > limit) {
                    long cleaned = clean(waiting, count);
                    if (cleaned != 0) {
                        bytesInRedis -= cleaned;
                    } else {
                        try {
                            sleep(250);
                        } catch (InterruptedException ie) {
                            Thread.interrupted();
                        }
                    }
                }

                BufferedInputStream in = null;
                try {
                    in = new BufferedInputStream(new FileInputStream(logFile));
                    int read = 0;
                    byte[] buffer = new byte[8192];
                    // 将文件上传至Redis
                    while ((read = in.read(buffer, 0, buffer.length)) != -1) {
                        if (buffer.length != read) {
                            byte[] bytes = new byte[read];
                            System.arraycopy(buffer, 0, bytes, 0, read);
                            conn.append((channel + logFile).getBytes(), bytes);
                        } else {
                            conn.append((channel + logFile).getBytes(), buffer);
                        }
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                } finally {
                    try {
                        in.close();
                    } catch (Exception ignore) {
                    }
                }

                // 提醒监听者文件准备就绪
                sendMessage(conn, channel, "source", logFile.toString());

                // 对本地记录的Redis内存占用量相关信息进行更新
                bytesInRedis += fsize;
                waiting.addLast(logFile);
            }

            // 向监听者报告文件处理完毕
            sendMessage(conn, channel, "source", ":done");

            // 工作完成之后清理无用日志文件
            while (waiting.size() > 0) {
                long cleaned = clean(waiting, count);
                if (cleaned != 0) {
                    bytesInRedis -= cleaned;
                } else {
                    try {
                        sleep(250);
                    } catch (InterruptedException ie) {
                        Thread.interrupted();
                    }
                }
            }
        }

        private long clean(Deque<File> waiting, int count) {
            if (waiting.size() == 0) {
                return 0;
            }
            File w0 = waiting.getFirst();
            if (String.valueOf(count).equals(conn.get(channel + w0 + ":done"))) {
                conn.del(channel + w0, channel + w0 + ":done");
                return waiting.removeFirst().length();
            }
            return 0;
        }
    }
}