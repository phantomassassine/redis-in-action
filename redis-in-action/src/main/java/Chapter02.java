
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * @Date: 2024/7/4 16:38
 * @Description: Fake Web Retailer虚构的高负载大型网上商店
 * <p></p>
 *
 * 1. 使用Redis管理用户登录会话。<p>
 * 2. 使用Redis实现购物车。根据浏览过的商品、购物车中商品、最终购买商品进行统计计算。<p>
 * 3. 使用Redis缓存来减少数据库和Web前端负载<p>
 * 4. 使用Redis进行数据行缓存<p>
 * 5. 使用Redis缓存来减少实现页面缓存所需的内存数量<p>
 */

public class Chapter02 {
    public static final void main(String[] args)
            throws InterruptedException {
        new Chapter02().run();
    }

    public void run()
            throws InterruptedException {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testLoginCookies(conn);
        testShoppingCartCookies(conn);
        testCacheRows(conn);
        testCacheRequest(conn);
    }

    /**
     *
     * @param conn
     * @throws InterruptedException
     */
    public void testLoginCookies(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    public void testShoppingCartCookies(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testShopppingCartCookies -----");
        String token = UUID.randomUUID().toString();

        System.out.println("We'll refresh our session...");
        updateToken(conn, token, "username", "itemX");
        System.out.println("And add an item to the shopping cart");
        addToCart(conn, token, "itemY", 3);
        Map<String, String> r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        assert r.size() >= 1;

        System.out.println("Let's clean out our sessions and carts");
        CleanFullSessionsThread thread = new CleanFullSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart now contains:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    public void testCacheRows(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testCacheRows -----");
        System.out.println("First, let's schedule caching of itemX every 5 seconds");
        scheduleRowCache(conn, "itemX", 5);
        System.out.println("Our schedule looks like:");
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert s.size() != 0;

        System.out.println("We'll start a caching thread that will cache the data...");

        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        System.out.println("Our cached data looks like:");
        String r = conn.get("inv:itemX");
        System.out.println(r);
        assert r != null;
        System.out.println();

        System.out.println("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        System.out.println("Notice that the data has changed...");
        String r2 = conn.get("inv:itemX");
        System.out.println(r2);
        System.out.println();
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("Let's force un-caching");
        scheduleRowCache(conn, "itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        System.out.println("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The database caching thread is still alive?!?");
        }
    }

    public void testCacheRequest(Jedis conn) {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback() {
            public String call(String request) {
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);
        String result = cacheRequest(conn, url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !canCache(conn, "http://test.com/");
        assert !canCache(conn, "http://test.com/?item=itemX&_=1234536");
    }

    /**
     * 检查登录cookie的方法
     * @param conn
     * @param token
     * @return
     */
    public String checkToken(Jedis conn, String token) {
        // 尝试获取并返回令牌对应的用户
        return conn.hget("login:", token);
    }

    /**
     * 更新令牌，记录用户最近浏览记录
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        // 维持令牌与已登陆用户之间的映射
        conn.hset("login:", token, user);
        // 记录令牌最后一次出现的时间
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            // 记录浏览过的商品
            conn.zadd("viewed:" + token, timestamp, item);
            // 保留最近25条浏览记录
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            // 记录所有商品浏览次数并对浏览次数进行排序
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 更新（移除、添加）购物车
     * 购物车数据结构：散列hash，存储了商品Id与订购数量之间的映射（id-count）
     * @param conn
     * @param session
     * @param item
     * @param count
     */
    public void addToCart(Jedis conn, String session, String item, int count) {
        if (count <= 0) {
            conn.hdel("cart:" + session, item);
        } else {
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
    }

    /**
     * 负责·调度·缓存和终止缓存的函数
     * 数据结构：使用两个<strong>有序集合</strong>来记录何时对缓存进行更新
     *      schedule{"数据行ID":"时间戳", ...} ——何时将指定的数据行缓存
     *      delay{"数据行ID":"间隔", ...} ——指定行缓存需要每隔多久更新一回
     * @param conn
     * @param rowId
     * @param delay
     */
    public void scheduleRowCache(Jedis conn, String rowId, int delay) {
        // 设置缓存数据行的延迟值 delay
        conn.zadd("delay:", delay, rowId);
        // 立即对需要缓存的数据进行调度 schedule
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    /**
     * 网页缓存
     * @param conn
     * @param request
     * @param callback
     * @return
     */
    public String cacheRequest(Jedis conn, String request, Callback callback) {
        if (!canCache(conn, request)) {
            return callback != null ? callback.call(request) : null;
        }

        // 将请求转换成简单字符串键方便后续查找
        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);

        // 尝试查找缓存，没有被缓存则生成页面并放入缓存
        if (content == null && callback != null) {
            content = callback.call(request);
            conn.setex(pageKey, 300, content);
        }

        return content;
    }

    public boolean canCache(Jedis conn, String request) {
        try {
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<String, String>();
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        } catch (MalformedURLException mue) {
            return false;
        }
    }

    public boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    public String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    public interface Callback {
        public String call(String request);
    }

    /**
     * 定期清理旧会话数据
     * @version 1.0
     */
    public class CleanSessionsThread
            extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                // 获取需要移除的令牌ID
                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                // 为将要被删除的令牌构建键名
                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }

                // 移除最旧的令牌
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }

    /**
     * 清理就会话的同时一并删除对应用户的购物车
     * @version 2.0
     */
    public class CleanFullSessionsThread
            extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanFullSessionsThread(int limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String sess : sessions) {
                    sessionKeys.add("viewed:" + sess);
                    sessionKeys.add("cart:" + sess);    // 删除旧对话对应账户的购物车
                }

                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }

    /**
     * 守护进程
     * 重复进行调度的数据行自动缓存机制
     */
    public class CacheRowsThread
            extends Thread {
        private Jedis conn;
        private boolean quit;

        public CacheRowsThread() {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            Gson gson = new Gson();
            while (!quit) {
                // 尝试获取下一个需要被缓存的数据行以及该行的调度时间戳
                Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                // 没有行需要被缓存则休眠后尝试
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                // 是否需要被缓存
                String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);
                if (delay <= 0) {
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                // 更新调度时间并设置缓存值
                Inventory row = Inventory.get(rowId);
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory(String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}