import java.util.List;

import redis.clients.jedis.Jedis;

public class JedisClientManager
{
	public static void main(String[] args)
	{

		// ���ӱ��ص�Redis����
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		
		// �洢List��������
		jedis.lpush("test-list", "Java");
		jedis.lpush("test-list", "PHP");
		jedis.lpush("test-list", "C++");
		// ��ȡlist��������
		List<String> listCache = jedis.lrange("test-list", 0, 3);
		for (int i = 0; i < listCache.size(); i++)
		{
			System.out.println("���������" + listCache.get(i));
		}

		jedis.close();
	}
}
