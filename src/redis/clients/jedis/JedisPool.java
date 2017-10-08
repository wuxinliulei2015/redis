package redis.clients.jedis;

import java.net.URI;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;
import redis.clients.util.Pool;

/**
 * 创建JedisPool时可以指定自定义实现的GenericObjectPoolConfig对象 用法如下:<br>
 * JedisPoolConfig config = new JedisPoolConfig();
 * <table border>
 * <tr>
 * <td>
 * config.setEvictionPolicyClassName
 * ("org.apache.commons.pool2.impl.DefaultEvictionPolicy");</td>
 * <td>设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)</td>
 * </tr>
 * 
 * <tr>
 * <td>
 * config.setBlockWhenExhausted(true)</td>
 * <td>
 * 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true</td>
 * </tr>
 * </table>
 * 
 * 是否启用pool的jmx管理功能, 默认true<br>
 * config.setJmxEnabled(true);<br>
 * MBean ObjectName = new
 * ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" +
 * i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好. config.setJmxNamePrefix("pool");
 * 
 * //是否启用后进先出, 默认true config.setLifo(true);
 * 
 * //最大空闲连接数, 默认8个 config.setMaxIdle(8);
 * 
 * //最大连接数, 默认8个 config.setMaxTotal(8);
 * 
 * //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间, 默认-1
 * config.setMaxWaitMillis(-1);
 * 
 * //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
 * config.setMinEvictableIdleTimeMillis(1800000);
 * 
 * //最小空闲连接数, 默认0 config.setMinIdle(0);
 * 
 * //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
 * config.setNumTestsPerEvictionRun(3);
 * 
 * //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断
 * (默认逐出策略) config.setSoftMinEvictableIdleTimeMillis(1800000);
 * 
 * //在获取连接的时候检查有效性, 默认false config.setTestOnBorrow(false);
 * 
 * //在空闲时检查有效性, 默认false config.setTestWhileIdle(false);
 * 
 * //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
 * config.setTimeBetweenEvictionRunsMillis(-1);
 * 
 * JedisPool pool = new JedisPool(config, "localhost",);
 * 
 * int timeout=3000; new JedisSentinelPool(master, sentinels,
 * poolConfig,timeout);//timeout 读取超时
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
	 * 从JedisPool池中取出一个jedis对象
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
