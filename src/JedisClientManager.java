import java.util.List;

import redis.clients.jedis.Jedis;

public class JedisClientManager
{
	public static void main(String[] args)
	{

		// 连接本地的Redis服务
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		
		// 存储List缓存数据
		jedis.lpush("test-list", "Java");
		jedis.lpush("test-list", "PHP");
		jedis.lpush("test-list", "C++");
		// 获取list缓存数据
		List<String> listCache = jedis.lrange("test-list", 0, 3);
		for (int i = 0; i < listCache.size(); i++)
		{
			System.out.println("缓存输出：" + listCache.get(i));
		}

		jedis.close();
	}
}
