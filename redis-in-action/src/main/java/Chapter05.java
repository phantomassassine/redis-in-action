import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.csv.CSVParser;
import org.javatuples.Pair;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileReader;
import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Date: 2024/7/9 15:22
 * @Description: 构建组件以支持持续运行的更高层次的应用程序 <p></p>
 * 1. 使用Redis记录最新日志和最常见日志<br>
 * 2. 使用Redis实现计数器并进行数据统计<br>
 * 3. 查询IP地址所属的城市与国家<br>
 * 4. 服务的发现与配置<br>
 */

public class Chapter05 {
    /**
     * 字典，将大部分日志的安全级别映射为字符串
     */
    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP =
            new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");
    private static final SimpleDateFormat ISO_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");
    static{
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static final void main(String[] args)
            throws InterruptedException
    {
        new Chapter05().run();
    }

    public void run()
            throws InterruptedException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testLogRecent(conn);
        testLogCommon(conn);
        testCounters(conn);
        testStats(conn);
        testAccessTime(conn);
        testIpLookup(conn);
        testIsUnderMaintenance(conn);
        testConfig(conn);
    }

    public void testLogRecent(Jedis conn) {
        System.out.println("\n----- testLogRecent -----");
        System.out.println("Let's write a few logs to the recent log");
        for (int i = 0; i < 5; i++) {
            logRecent(conn, "test", "this is message " + i);
        }
        List<String> recent = conn.lrange("recent:test:info", 0, -1);
        System.out.println(
                "The current recent message log has this many messages: " +
                        recent.size());
        System.out.println("Those messages include:");
        for (String message : recent){
            System.out.println(message);
        }
        assert recent.size() >= 5;
    }

    public void testLogCommon(Jedis conn) {
        System.out.println("\n----- testLogCommon -----");
        System.out.println("Let's write some items to the common log");
        for (int count = 1; count < 6; count++) {
            for (int i = 0; i < count; i ++) {
                logCommon(conn, "test", "message-" + count);
            }
        }
        Set<Tuple> common = conn.zrevrangeWithScores("common:test:info", 0, -1);
        System.out.println("The current number of common messages is: " + common.size());
        System.out.println("Those common messages are:");
        for (Tuple tuple : common){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert common.size() >= 5;
    }

    public void testCounters(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testCounters -----");
        System.out.println("Let's update some counters for now and a little in the future");
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++) {
            int count = (int)(Math.random() * 5) + 1;
            updateCounter(conn, "test", count, now + i);
        }

        List<Pair<Integer,Integer>> counter = getCounter(conn, "test", 1);
        System.out.println("We have some per-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer,Integer> count : counter){
            System.out.println("  " + count);
        }
        assert counter.size() >= 10;

        counter = getCounter(conn, "test", 5);
        System.out.println("We have some per-5-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer,Integer> count : counter){
            System.out.println("  " + count);
        }
        assert counter.size() >= 2;
        System.out.println();

        System.out.println("Let's clean out some counters by setting our sample count to 0");
        CleanCountersThread thread = new CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter(conn, "test", 86400);
        System.out.println("Did we clean out all of the counters? " + (counter.size() == 0));
        assert counter.size() == 0;
    }

    public void testStats(Jedis conn) {
        System.out.println("\n----- testStats -----");
        System.out.println("Let's add some data for our statistics!");
        List<Object> r = null;
        for (int i = 0; i < 5; i++){
            double value = (Math.random() * 11) + 5;
            r = updateStats(conn, "temp", "example", value);
        }
        System.out.println("We have some aggregate statistics: " + r);
        Map<String,Double> stats = getStats(conn, "temp", "example");
        System.out.println("Which we can also fetch manually:");
        System.out.println(stats);
        assert stats.get("count") >= 5;
    }

    public void testAccessTime(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testAccessTime -----");
        System.out.println("Let's calculate some access times...");
        AccessTimer timer = new AccessTimer(conn);
        for (int i = 0; i < 10; i++){
            timer.start();
            Thread.sleep((int)((.5 + Math.random()) * 1000));
            timer.stop("req-" + i);
        }
        System.out.println("The slowest access times are:");
        Set<Tuple> atimes = conn.zrevrangeWithScores("slowest:AccessTime", 0, -1);
        for (Tuple tuple : atimes){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert atimes.size() >= 10;
        System.out.println();
    }

    public void testIpLookup(Jedis conn) {
        System.out.println("\n----- testIpLookup -----");
        String cwd = System.getProperty("user.dir");
        File blocks = new File(cwd + "/GeoLiteCity-Blocks.csv");
        File locations = new File(cwd + "/GeoLiteCity-Location.csv");
        if (!blocks.exists()){
            System.out.println("********");
            System.out.println("GeoLiteCity-Blocks.csv not found at: " + blocks);
            System.out.println("********");
            return;
        }
        if (!locations.exists()){
            System.out.println("********");
            System.out.println("GeoLiteCity-Location.csv not found at: " + locations);
            System.out.println("********");
            return;
        }

        System.out.println("Importing IP addresses to Redis... (this may take a while)");
        importIpsToRedis(conn, blocks);
        long ranges = conn.zcard("ip2cityid:");
        System.out.println("Loaded ranges into Redis: " + ranges);
        assert ranges > 1000;
        System.out.println();

        System.out.println("Importing Location lookups to Redis... (this may take a while)");
        importCitiesToRedis(conn, locations);
        long cities = conn.hlen("cityid2city:");
        System.out.println("Loaded city lookups into Redis:" + cities);
        assert cities > 1000;
        System.out.println();

        System.out.println("Let's lookup some locations!");
        for (int i = 0; i < 5; i++){
            String ip =
                    randomOctet(255) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256);
            System.out.println(Arrays.toString(findCityByIp(conn, ip)));
        }
    }

    public void testIsUnderMaintenance(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testIsUnderMaintenance -----");
        System.out.println("Are we under maintenance (we shouldn't be)? " + isUnderMaintenance(conn));
        conn.set("is-under-maintenance", "yes");
        System.out.println("We cached this, so it should be the same: " + isUnderMaintenance(conn));
        Thread.sleep(1000);
        System.out.println("But after a sleep, it should change: " + isUnderMaintenance(conn));
        System.out.println("Cleaning up...");
        conn.del("is-under-maintenance");
        Thread.sleep(1000);
        System.out.println("Should be False again: " + isUnderMaintenance(conn));
    }

    public void testConfig(Jedis conn) {
        System.out.println("\n----- testConfig -----");
        System.out.println("Let's set a config and then get a connection from that config...");
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("db", 15);
        setConfig(conn, "redis", "test", config);

        Jedis conn2 = redisConnection("test");
        System.out.println(
                "We can run commands from the configured connection: " + (conn2.info() != null));
    }

    /**
     * 记录最新出现日志
     *  - LPUSH命令将日志消息推入到列表里
     *  - LRANGE命令取出列表中的消息
     * @param conn
     * @param name
     * @param message
     */
    public void logRecent(Jedis conn, String name, String message) {
        logRecent(conn, name, message, INFO);
    }

    /**
     * LPUSH LTRIM操作记录最新出现的日志
     * @param conn
     * @param name
     * @param message
     * @param severity
     */
    public void logRecent(Jedis conn, String name, String message, String severity) {
        // 创建负责存储消息的键
        String destination = "recent:" + name + ':' + severity;
        // 使用流水线将通信往返次数降低为一次
        Pipeline pipe = conn.pipelined();
        // 将消息添加到日志列表的最前面
        pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message);
        // 修建日志列表保持100条消息
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }

    /**
     * 记录并轮换最常见日志
     * @param conn
     * @param name
     * @param message
     */
    public void logCommon(Jedis conn, String name, String message) {
        logCommon(conn, name, message, INFO, 5000);
    }

    /**
     * 将消息作为成员存储到有序集合里，并将消息出现的频率设置为成员的分值。
     * 常见日志函数需要更谨慎地处理上一小时收集到的日志：
     * - 在WATCH/MULTI/EXEC事务里对上一小时常见日志的有序集合改名，并更新当前小时数的键。
     * - 将流水线对象传递给logRecent()函数以减少服务器之间的通信往返次数
     * @param conn
     * @param name
     * @param message
     * @param severity
     * @param timeout
     */
    public void logCommon(
            Jedis conn, String name, String message, String severity, int timeout) {
        // 负责存储近期的常见日志消息的键
        String commonDest = "common:" + name + ':' + severity;
        // 记录当前所处的小时数
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            // 对记录当前的小时数的键进行监视
            conn.watch(startKey);
            // 当前所处小时数
            String hourStart = ISO_FORMAT.format(new Date());
            String existing = conn.get(startKey);

            // 创建事务
            Transaction trans = conn.multi();
            // 若该日志消息列表记录的是上一个小时的日志，则归档旧日志消息
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0){
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + ":pstart");
                trans.set(startKey, hourStart);
            }

            // 对记录日志出现的次数的计数器自增
            trans.zincrby(commonDest, 1, message);

            // logRecent()函数
            String recentDest = "recent:" + name + ':' + severity;
            trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return;
        }
    }

    /**
     * 程序更新计数器的方法
     * @param conn
     * @param name
     * @param count
     */
    public void updateCounter(Jedis conn, String name, int count) {
        updateCounter(conn, name, count, System.currentTimeMillis() / 1000);
    }

    /**
     * 以秒为单位的计数器精度，可以按需调整
     */
    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    /**
     * 更新计数器信息：
     *  - 对于每种时间片精度，将计数器精度和名字作为引用信息添加到已有计数器的有序集合里ZADD，
     *  - 并增加散列计数器在指定时间片内的计数值HINCRBY。
     * <p></p>
     * 数据结构：
     * 1. 散列--存储网站在每个时间片之内获得的点击量
     *      count{"timestamp":"hits", ...}
     * 2. 有序集合--对被使用的计数器进行记录，需要有序序列。分值置为0
     *      known{"cntPrecision+cntName":0}
     * @param conn
     * @param name
     * @param count
     * @param now
     */
    public void updateCounter(Jedis conn, String name, int count, long now){
        // 创建事务型流水线，以保证后续清理工作正确执行
        Transaction trans = conn.multi();
        // 为每种精度都创建一个计数器
        for (int prec : PRECISION) {
            // 取得当前时间片的开始时间
            long pnow = (now / prec) * prec;
            // 创建存储计数信息的散列
            String hash = String.valueOf(prec) + ':' + name;
            // 将计数器引用信息添加到有序集合里
            trans.zadd("known:", 0, hash);
            // 更新给定名字和精度的计数器
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    /**
     * 从指定精度和名字的计数器里获取数据，
     * 并将其转换成整数然后根据时间先后进行排序
     * @param conn
     * @param name
     * @param precision
     * @return
     */
    public List<Pair<Integer,Integer>> getCounter(
            Jedis conn, String name, int precision)
    {
        String hash = String.valueOf(precision) + ':' + name;
        // HGETALL命令取出计数器数据
        Map<String,String> data = conn.hgetAll("count:" + hash);
        ArrayList<Pair<Integer,Integer>> results =
                new ArrayList<Pair<Integer,Integer>>();
        // 将计数器数据转换成指定格式
        for (Map.Entry<String,String> entry : data.entrySet()) {
            results.add(new Pair<Integer,Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())));
        }
        // 排序将旧数据样本排在前面
        Collections.sort(results);
        return results;
    }

    /**
     * 更新统计数据代码：
     *  - 写入数据前进行检查，对不属于当前一小时的旧数据归档，
     *  - 构建两个临时有序集合用于保存最小最大值，
     *  - 使用ZUNIONSTORE命令及MIN和MAX函数计算临时有序集合和当前有序集合之间的并集
     * <p></p>
     * 数据结构：
     * 1. 有序集合（为了求并集）--页面访问时间统计
     *      statsProfilePageAccessTime{"min":"val", "max":"val, "sumsq":"val", "count":"val, ...}
     *
     * @param conn
     * @param context
     * @param type
     * @param value
     * @return
     */
    public List<Object> updateStats(Jedis conn, String context, String type, double value){
        int timeout = 5000;
        // 负责存储统计数据的键
        String destination = "stats:" + context + ':' + type;
        // 类似commonLog()函数，处理当前一小时和上一小时的数据
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0){
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }

            // 将值添加到临时键里
            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            trans.zadd(tkey1, value, "min");
            trans.zadd(tkey2, value, "max");

            // 使用ZUNIONSTORE命令快速更新统计数据而无需使用WATCH监视频繁更新的键
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MIN),
                    destination, tkey1);
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    destination, tkey2);

            trans.del(tkey1, tkey2);
            trans.zincrby(destination, 1, "count");
            trans.zincrby(destination, value, "sum");
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> results = trans.exec();
            if (results == null){
                continue;
            }
            return results.subList(results.size() - 3, results.size());
        }
        return null;
    }

    /**
     * 取出统计数据
     * @param conn
     * @param context
     * @param type
     * @return
     */
    public Map<String,Double> getStats(Jedis conn, String context, String type){
        String key = "stats:" + context + ':' + type;
        Map<String,Double> stats = new HashMap<String,Double>();
        // 获取基本统计数据
        Set<Tuple> data = conn.zrangeWithScores(key, 0, -1);
        for (Tuple tuple : data){
            stats.put(tuple.getElement(), tuple.getScore());
        }
        // 平均值
        stats.put("average", stats.get("sum") / stats.get("count"));
        // 方差
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"), 2) / stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return stats;
    }

    private long lastChecked;
    private boolean underMaintenance;

    /**
     * 每秒进行一次服务器维护状态信息的更新。
     * 模拟将配置信息存储在一个普遍可访问位置。
     * @param conn
     * @return
     */
    public boolean isUnderMaintenance(Jedis conn) {
        // 每秒检查一次
        if (lastChecked < System.currentTimeMillis() - 1000){
            lastChecked = System.currentTimeMillis();
            // 检查系统是否进行维护
            String flag = conn.get("is-under-maintenance");
            underMaintenance = "yes".equals(flag);
        }

        // 系统是否正在进行维护
        return underMaintenance;
    }

    public void setConfig(
            Jedis conn, String type, String component, Map<String,Object> config) {
        Gson gson = new Gson();
        conn.set("config:" + type + ':' + component, gson.toJson(config));
    }

    private static final Map<String,Map<String,Object>> CONFIGS =
            new HashMap<String,Map<String,Object>>();
    private static final Map<String,Long> CHECKED = new HashMap<String,Long>();

    /**
     * 按需对配置信息进行局部缓存
     * @param conn
     * @param type
     * @param component
     * @return
     */
    @SuppressWarnings("unchecked")
    public Map<String,Object> getConfig(Jedis conn, String type, String component) {
        int wait = 1000;
        String key = "config:" + type + ':' + component;

        Long lastChecked = CHECKED.get(key);
        if (lastChecked == null || lastChecked < System.currentTimeMillis() - wait){
            CHECKED.put(key, System.currentTimeMillis());

            // 检查组件配置
            String value = conn.get(key);
            Map<String,Object> config = null;
            if (value != null){
                Gson gson = new Gson();
                config = (Map<String,Object>)gson.fromJson(
                        value, new TypeToken<Map<String,Object>>(){}.getType());
            }else{
                config = new HashMap<String,Object>();
            }

            CONFIGS.put(key, config);
        }

        return CONFIGS.get(key);
    }

    public static final Map<String,Jedis> REDIS_CONNECTIONS =
            new HashMap<String,Jedis>();

    /**
     * decorator装饰器：
     * 接受一个指定配置作为参数并生成一个包装器wrapper包裹起一个函数，
     * 使得被包裹函数的调用可以自动连接至正确的Redis服务器，
     * 且连接会和用户之后提供的其他参数一同传递至被包裹函数
     * @param component
     * @return
     */
    public Jedis redisConnection(String component){
        Jedis configConn = REDIS_CONNECTIONS.get("config");
        if (configConn == null){
            configConn = new Jedis("localhost");
            configConn.select(15);
            REDIS_CONNECTIONS.put("config", configConn);
        }

        String key = "config:redis:" + component;
        Map<String,Object> oldConfig = CONFIGS.get(key);
        Map<String,Object> config = getConfig(configConn, "redis", component);

        if (!config.equals(oldConfig)){
            Jedis conn = new Jedis("localhost");
            if (config.containsKey("db")){
                conn.select(((Double)config.get("db")).intValue());
            }
            REDIS_CONNECTIONS.put(key, conn);
        }

        return REDIS_CONNECTIONS.get(key);
    }

    /**
     * 创建IP地址与城市ID之间的映射
     * <p></p>
     * 数据结构：
     *  1. 有序集合
     *      ip2cityId{"cityId":"IP", ...}
     * @param conn
     * @param file 需要GeoLiteCity-Blocks.csv文件即IP所属城市数据库
     */
    public void importIpsToRedis(Jedis conn, File file) {
        FileReader reader = null;
        try{
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            int count = 0;
            String[] line = null;
            while ((line = parser.getLine()) != null){
                String startIp = line.length > 1 ? line[0] : "";
                if (startIp.toLowerCase().indexOf('i') != -1){
                    continue;
                }
                // 按需将IP地址转换为分值
                int score = 0;
                if (startIp.indexOf('.') != -1){
                    score = ipToScore(startIp);
                }else{
                    try{
                        score = Integer.parseInt(startIp, 10);
                    }catch(NumberFormatException nfe){
                        continue;
                    }
                }

                // 构建唯一城市ID
                String cityId = line[2] + '_' + count;
                // 将个城市ID及其对应IP地址分值添加到有序集合
                conn.zadd("ip2cityid:", score, cityId);
                count++;
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try{
                reader.close();
            }catch(Exception e){
                // ignore
            }
        }
    }

    /**
     * 创建一个城市ID映射至城市信息散列
     * @param conn
     * @param file GeoLiteCity-Blocks.csv
     */
    public void importCitiesToRedis(Jedis conn, File file) {
        Gson gson = new Gson();
        FileReader reader = null;
        try{
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            String[] line = null;
            while ((line = parser.getLine()) != null){
                if (line.length < 4 || !Character.isDigit(line[0].charAt(0))){
                    continue;
                }
                String cityId = line[0];
                String country = line[1];
                String region = line[2];
                String city = line[3];
                // 编码为JSON列表后进行存储
                String json = gson.toJson(new String[]{city, region, country});
                conn.hset("cityid2city:", cityId, json);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try{
                reader.close();
            }catch(Exception e){
                // ignore
            }
        }
    }

    /**
     * IP地址转换
     * @param ipAddress
     * @return
     */
    public int ipToScore(String ipAddress) {
        int score = 0;
        for (String v : ipAddress.split("\\.")){
            score = score * 256 + Integer.parseInt(v, 10);
        }
        return score;
    }

    public String randomOctet(int max) {
        return String.valueOf((int)(Math.random() * max));
    }

    /**
     * IP地址所属地查找
     * @param conn
     * @param ipAddress
     * @return
     */
    public String[] findCityByIp(Jedis conn, String ipAddress) {
        int score = ipToScore(ipAddress);
        // ZREVERANGEBYSCORE 查找唯一城市ID
        Set<String> results = conn.zrevrangeByScore("ip2cityid:", score, 0, 0, 1);
        if (results.size() == 0) {
            return null;
        }

        // 唯一城市ID转换为普通 城市ID
        String cityId = results.iterator().next();
        cityId = cityId.substring(0, cityId.indexOf('_'));
        // 从散列中取出城市信息
        return new Gson().fromJson(conn.hget("cityid2city:", cityId), String[].class);
    }

    /**
     * 一个接一个地遍历有序集合里面记录的计数器，
     * 查找需要进行清理的计数器
     */
    public class CleanCountersThread
            extends Thread
    {

        private Jedis conn;
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset; // used to mimic a time in the future.

        public CleanCountersThread(int sampleCount, long timeOffset){
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit(){
            quit = true;
        }

        public void run(){
            // 记录清理操作执行次数
            int passes = 0;
            while (!quit){
                // 记录清理操作开始执行的时间
                long start = System.currentTimeMillis() + timeOffset;
                // 逐渐遍历所有已知计数器
                int index = 0;
                while (index < conn.zcard("known:")){
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }
                    String hash = hashSet.iterator().next();
                    // 计数器精度
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(':')));
                    // 是否有必要清理
                    int bprec = (int)Math.floor(prec / 60);
                    if (bprec == 0){
                        bprec = 1;
                    }
                    if ((passes % bprec) != 0){
                        continue;
                    }

                    String hkey = "count:" + hash;
                    // 获取样本开始时间
                    String cutoff = String.valueOf(
                            ((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(conn.hkeys(hkey));
                    Collections.sort(samples);
                    // 计算要移除的样本数量
                    int remove = bisectRight(samples, cutoff);

                    // 按需移除计数样本
                    if (remove != 0){
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size()){
                            // 在尝试修改计数器散列之前对其进行监视
                            // 验证计数器散列是否为空，决定是否从有序集合里删除
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0) {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            }else{
                                conn.unwatch();
                            }
                        }
                    }
                }

                // 对记录循环次数的变量以及记录执行时长的变量进行更新
                passes++;
                long duration = Math.min(
                        (System.currentTimeMillis() + timeOffset) - start + 1000, 60000);
                // 执行清理而预留的时间
                try {
                    sleep(Math.max(60000 - duration, 1000));
                }catch(InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
            }
        }

        // mimic python's bisect.bisect_right
        public int bisectRight(List<String> values, String key) {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }

    /**
     * 上下文管理器
     */
    public class AccessTimer {
        private Jedis conn;
        private long start;

        public AccessTimer(Jedis conn){
            this.conn = conn;
        }

        public void start(){
            start = System.currentTimeMillis();
        }

        public void stop(String context){
            // 代码块执行时长
            long delta = System.currentTimeMillis() - start;
            List<Object> stats = updateStats(conn, context, "AccessTime", delta / 1000.0);
            double average = (Double)stats.get(1) / (Double)stats.get(0);

            // 将页面平均访问市场添加到记录最常访问时间的有序集合里
            Transaction trans = conn.multi();
            trans.zadd("slowest:AccessTime", average, context);
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }
}
