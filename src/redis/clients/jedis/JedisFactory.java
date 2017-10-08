package redis.clients.jedis;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;

/**
 * PoolableObjectFactory custom impl.
 * JedisPool在产生和销毁JedisPool对象的时候都会调用这个工厂的相应处理方法
 */
class JedisFactory implements PooledObjectFactory<Jedis>
{
	private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
	private final int connectionTimeout;
	private final int soTimeout;
	private final String password;
	private final int database;
	private final String clientName;

	public JedisFactory(final String host, final int port, final int connectionTimeout, final int soTimeout,
			final String password, final int database, final String clientName)
	{
		this.hostAndPort.set(new HostAndPort(host, port));
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.password = password;
		this.database = database;
		this.clientName = clientName;
	}

	public JedisFactory(final URI uri, final int connectionTimeout, final int soTimeout, final String clientName)
	{
		if (!JedisURIHelper.isValid(uri))
		{
			throw new InvalidURIException(String.format("Cannot open Redis connection due invalid URI. %s", uri.toString()));
		}

		this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.password = JedisURIHelper.getPassword(uri);
		this.database = JedisURIHelper.getDBIndex(uri);
		this.clientName = clientName;
	}

	public void setHostAndPort(final HostAndPort hostAndPort)
	{
		this.hostAndPort.set(hostAndPort);
	}

	/**
	 * 激活对象
	 */
	@Override
	public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		final BinaryJedis jedis = pooledJedis.getObject();
		if (jedis.getDB() != database)
		{
			jedis.select(database);
		}

	}

	/**
	 * 销毁对象
	 */
	@Override
	public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		final BinaryJedis jedis = pooledJedis.getObject();
		if (jedis.isConnected())
		{
			try
			{
				try
				{
					jedis.quit();
				}
				catch (Exception e)
				{
				}
				jedis.disconnect();
			}
			catch (Exception e)
			{

			}
		}

	}

	/**
	 * 生成对象时调用
	 */
	@Override
	public PooledObject<Jedis> makeObject() throws Exception
	{
		// 根据JedisPool的构造参数传递进来的host和port
		final HostAndPort hostAndPort = this.hostAndPort.get();
		final Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), connectionTimeout, soTimeout);

		try
		{
			jedis.connect();
			if (null != this.password)
			{
				jedis.auth(this.password);
			}
			if (database != 0)
			{
				jedis.select(database);
			}
			if (clientName != null)
			{
				jedis.clientSetname(clientName);
			}
		}
		catch (JedisException je)
		{
			jedis.close();
			throw je;
		}

		return new DefaultPooledObject<Jedis>(jedis);

	}

	@Override
	public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception
	{
		// TODO maybe should select db 0? Not sure right now.
	}

	@Override
	public boolean validateObject(PooledObject<Jedis> pooledJedis)
	{
		final BinaryJedis jedis = pooledJedis.getObject();
		try
		{
			HostAndPort hostAndPort = this.hostAndPort.get();

			String connectionHost = jedis.getClient().getHost();
			int connectionPort = jedis.getClient().getPort();

			return hostAndPort.getHost().equals(connectionHost) && hostAndPort.getPort() == connectionPort
					&& jedis.isConnected() && jedis.ping().equals("PONG");
		}
		catch (final Exception e)
		{
			return false;
		}
	}
}