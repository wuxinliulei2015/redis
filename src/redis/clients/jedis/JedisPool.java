package redis.clients.jedis;

import java.net.URI;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;
import redis.clients.util.Pool;

/**
 * ����JedisPoolʱ����ָ���Զ���ʵ�ֵ�GenericObjectPoolConfig���� �÷�����:<br>
 * JedisPoolConfig config = new JedisPoolConfig();
 * <table border>
 * <tr>
 * <td>
 * config.setEvictionPolicyClassName
 * ("org.apache.commons.pool2.impl.DefaultEvictionPolicy");</td>
 * <td>���õ������������, Ĭ��DefaultEvictionPolicy(�����ӳ���������ʱ��,������������������������)</td>
 * </tr>
 * 
 * <tr>
 * <td>
 * config.setBlockWhenExhausted(true)</td>
 * <td>
 * ���Ӻľ�ʱ�Ƿ�����, false���쳣,ture����ֱ����ʱ, Ĭ��true</td>
 * </tr>
 * </table>
 * 
 * �Ƿ�����pool��jmx������, Ĭ��true<br>
 * config.setJmxEnabled(true);<br>
 * MBean ObjectName = new
 * ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" +
 * i); Ĭ ��Ϊ"pool", JMX����,���岻֪���Ǹ�ɶ��...Ĭ�Ͼͺ�. config.setJmxNamePrefix("pool");
 * 
 * //�Ƿ����ú���ȳ�, Ĭ��true config.setLifo(true);
 * 
 * //������������, Ĭ��8�� config.setMaxIdle(8);
 * 
 * //���������, Ĭ��8�� config.setMaxTotal(8);
 * 
 * //��ȡ����ʱ�����ȴ�������(�������Ϊ����ʱBlockWhenExhausted),�����ʱ�����쳣, С����:������ȷ����ʱ��, Ĭ��-1
 * config.setMaxWaitMillis(-1);
 * 
 * //������ӵ���С����ʱ�� Ĭ��1800000����(30����)
 * config.setMinEvictableIdleTimeMillis(1800000);
 * 
 * //��С����������, Ĭ��0 config.setMinIdle(0);
 * 
 * //ÿ��������ʱ ����������Ŀ ���Ϊ�������� : 1/abs(n), Ĭ��3
 * config.setNumTestsPerEvictionRun(3);
 * 
 * //������ж�ú����, ������ʱ��>��ֵ �� ��������>�������� ʱֱ�����,���ٸ���MinEvictableIdleTimeMillis�ж�
 * (Ĭ���������) config.setSoftMinEvictableIdleTimeMillis(1800000);
 * 
 * //�ڻ�ȡ���ӵ�ʱ������Ч��, Ĭ��false config.setTestOnBorrow(false);
 * 
 * //�ڿ���ʱ�����Ч��, Ĭ��false config.setTestWhileIdle(false);
 * 
 * //���ɨ���ʱ����(����) ���Ϊ����,����������߳�, Ĭ��-1
 * config.setTimeBetweenEvictionRunsMillis(-1);
 * 
 * JedisPool pool = new JedisPool(config, "localhost",);
 * 
 * int timeout=3000; new JedisSentinelPool(master, sentinels,
 * poolConfig,timeout);//timeout ��ȡ��ʱ
 * 
 * @author Administrator
 * 
 */
public class JedisPool extends Pool<Jedis>
{

	public JedisPool()
	{
		this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host)
	{
		this(poolConfig, host, Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
	}

	public JedisPool(String host, int port)
	{
		this(new GenericObjectPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
	}

	public JedisPool(final String host)
	{
		URI uri = URI.create(host);
		if (JedisURIHelper.isValid(uri))
		{
			String h = uri.getHost();
			int port = uri.getPort();
			String password = JedisURIHelper.getPassword(uri);
			int database = JedisURIHelper.getDBIndex(uri);
			this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(h, port, Protocol.DEFAULT_TIMEOUT,
					Protocol.DEFAULT_TIMEOUT, password, database, null), new GenericObjectPoolConfig());
		}
		else
		{
			this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(host, Protocol.DEFAULT_PORT,
					Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null),
					new GenericObjectPoolConfig());
		}
	}

	public JedisPool(final URI uri)
	{
		this(new GenericObjectPoolConfig(), uri, Protocol.DEFAULT_TIMEOUT);
	}

	public JedisPool(final URI uri, final int timeout)
	{
		this(new GenericObjectPoolConfig(), uri, timeout);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, int timeout, final String password)
	{
		this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, null);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port)
	{
		this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port, final int timeout)
	{
		this(poolConfig, host, port, timeout, null, Protocol.DEFAULT_DATABASE, null);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, int timeout, final String password,
			final int database)
	{
		this(poolConfig, host, port, timeout, password, database, null);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, int timeout, final String password,
			final int database, final String clientName)
	{
		this(poolConfig, host, port, timeout, timeout, password, database, clientName);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, final int connectionTimeout,
			final int soTimeout, final String password, final int database, final String clientName)
	{
		super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, password, database, clientName));
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri)
	{
		this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout)
	{
		this(poolConfig, uri, timeout, timeout);
	}

	public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int connectionTimeout, final int soTimeout)
	{
		super(poolConfig, new JedisFactory(uri, connectionTimeout, soTimeout, null));
	}

	/**
	 * ��JedisPool����ȡ��һ��jedis����
	 */
	@Override
	public Jedis getResource()
	{
		Jedis jedis = super.getResource();
		jedis.setDataSource(this);
		return jedis;
	}

	/**
	 * @deprecated starting from Jedis 3.0 this method will not be exposed.
	 *             Resource cleanup should be done using @see
	 *             {@link redis.clients.jedis.Jedis#close()}
	 */
	@Override
	@Deprecated
	public void returnBrokenResource(final Jedis resource)
	{
		if (resource != null)
		{
			returnBrokenResourceObject(resource);
		}
	}

	/**
	 * @deprecated starting from Jedis 3.0 this method will not be exposed.
	 *             Resource cleanup should be done using @see
	 *             {@link redis.clients.jedis.Jedis#close()}
	 */
	@Override
	@Deprecated
	public void returnResource(final Jedis resource)
	{
		if (resource != null)
		{
			try
			{
				resource.resetState();
				returnResourceObject(resource);
			}
			catch (Exception e)
			{
				returnBrokenResource(resource);
				throw new JedisException("Could not return the resource to the pool", e);
			}
		}
	}
}
