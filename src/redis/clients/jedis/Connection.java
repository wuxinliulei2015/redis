package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.IOUtils;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

/**
 * jedis与redis进行<B>TCP通信</B>的最基础类，建立TCP链接的实施者
 * <p>
 */
public class Connection implements Closeable
{

	private static final byte[][] EMPTY_ARGS = new byte[0][];

	/**
	 * 默认链接的ip:localhost
	 */
	private String host = Protocol.DEFAULT_HOST;
	/**
	 * 默认端口号：6379
	 */
	private int port = Protocol.DEFAULT_PORT;

	/**
	 * TCP链接的socket对象
	 */
	private Socket socket;

	/**
	 * 该TCP链接的输出流，是OutputStream的装饰者类
	 */
	private RedisOutputStream outputStream;

	/**
	 * 该TCP链接的输入流，是InputStream的装饰者类
	 */
	private RedisInputStream inputStream;

	/**
	 * 每发送一个命令，该值加1 每收到一个返回，该值减1
	 */
	private int pipelinedCommands = 0;

	/**
	 * 默认超时时间：2000
	 */
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	private int soTimeout = Protocol.DEFAULT_TIMEOUT;
	private boolean broken = false;

	public Connection()
	{
	}

	public Connection(final String host)
	{
		this.host = host;
	}

	public Connection(final String host, final int port)
	{
		this.host = host;
		this.port = port;
	}

	public Socket getSocket()
	{
		return socket;
	}

	public int getConnectionTimeout()
	{
		return connectionTimeout;
	}

	public int getSoTimeout()
	{
		return soTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
	}

	public void setSoTimeout(int soTimeout)
	{
		this.soTimeout = soTimeout;
	}

	public void setTimeoutInfinite()
	{
		try
		{
			if (!isConnected())
			{
				connect();
			}
			socket.setSoTimeout(0);
		}
		catch (SocketException ex)
		{
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	public void rollbackTimeout()
	{
		try
		{
			socket.setSoTimeout(soTimeout);
		}
		catch (SocketException ex)
		{
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	protected Connection sendCommand(final Command cmd, final String... args)
	{
		final byte[][] bargs = new byte[args.length][];
		for (int i = 0; i < args.length; i++)
		{
			bargs[i] = SafeEncoder.encode(args[i]);
		}
		return sendCommand(cmd, bargs);
	}

	protected Connection sendCommand(final Command cmd)
	{
		return sendCommand(cmd, EMPTY_ARGS);
	}

	protected Connection sendCommand(final Command cmd, final byte[]... args)
	{
		try
		{
			connect();
			Protocol.sendCommand(outputStream, cmd, args);
			pipelinedCommands++;
			return this;
		}
		catch (JedisConnectionException ex)
		{
			/*
			 * When client send request which formed by invalid protocol, Redis
			 * send back error message before close connection. We try to read
			 * it to provide reason of failure.
			 */
			try
			{
				String errorMessage = Protocol.readErrorLineIfPossible(inputStream);
				if (errorMessage != null && errorMessage.length() > 0)
				{
					ex = new JedisConnectionException(errorMessage, ex.getCause());
				}
			}
			catch (Exception e)
			{
				/*
				 * Catch any IOException or JedisConnectionException occurred
				 * from InputStream#read and just ignore. This approach is safe
				 * because reading error message is optional and connection will
				 * eventually be closed.
				 */
			}
			// Any other exceptions related to connection?
			broken = true;
			throw ex;
		}
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(final String host)
	{
		this.host = host;
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(final int port)
	{
		this.port = port;
	}

	/**
	 * 在利用该链接发送redis操作命令前，都会调用，
	 * <p>
	 * 或用于建立链接，
	 * <p>
	 * 或检查链接是否仍然可用，不可用则重新建立链接
	 */
	public void connect()
	{
		if (!isConnected())
		{
			try
			{
				socket = new Socket();

				// 启用/禁用 SO_REUSEADDR 套接字选项。
				// 关闭 TCP 连接时，该连接可能在关闭后的一段时间内保持超时状态（通常称为 TIME_WAIT 状态或 2MSL
				// 等待状态）。对于使用已知套接字地址或端口的应用程序而言，如果存在处于超时状态的连接（包括地址和端口），
				// 可能不能将套接字绑定到所需的SocketAddress 上。
				// 使用bind(SocketAddress),绑定套接字前启用,SO_REUSEADDR,允许在上一个连接处于超时状态时绑定套接字。
				// 当创建 Socket 时，属性值 SO_REUSEADDR 的初始设置是默认关闭的;在绑定套接字后启用或禁用
				// SO_REUSEADDR 时的结果是不确定的,所以需要在绑定套接字之前设置该值。
				socket.setReuseAddress(true);

				// keepalive不是说TCP的长连接，当我们作为服务端，一个客户端连接上来，如果设置了keeplive为true，
				// 当对方没有发送任何数据过来，超过一个时间(看系统内核参数配置)，那么我们这边会发送一个ack探测包发到对方，
				// 探测双方的TCP/IP连接是否有效(对方可能断点，断网)。如果不设置，那么客户端宕机时，服务器永远也不知道客户端宕机了，
				// 仍然保存这个失效的连接。
				// 当然，在客户端也可以使用这个参数。客户端Socket会每隔段的时间（大约两个小时）就会利用空闲的连接向服务器发送一个数据包。
				// 这个数据包并没有其它的作用，只是为了检测一下服务器是否仍处于活动状态。如果服务器未响应这个数据包，在大约11分钟后，
				// 客户端Socket再发送一个数据包，如果在12分钟内，服务器还没响应，那么客户端Socket将关闭。
				// 如果将该Socket选项关闭，客户端Socket在服务器无效的情况下可能会长时间不会关闭。
				// 尽管keepalive的好处并不多，但是很多开发者提倡在更高层次的应用程序代码中控制超时设置和死的套接字。同时需要记住，
				// keepalive不允许你为探测套接字终点（endpoint）指定一个值。所以建议开发者使用的另一种比keepalive更好的解决方案是修改超时设置套接字选项。
				// 说白了：这个参数其实对应用层的程序而言没有什么用。可以通过应用层实现了解服务端或客户端状态，而决定是否继续维持该Socket。
				// Will monitor the TCP connection is valid
				socket.setKeepAlive(true);

				// Socket buffer Whetherclosed, to ensure timely delivery of
				// data
				socket.setTcpNoDelay(true);

				// Control calls close () method,the underlying socket is closed
				// immediately
				socket.setSoLinger(true, 0);

				socket.connect(new InetSocketAddress(host, port), connectionTimeout);

				// 启用/禁用带有指定超时值的 SO_TIMEOUT，以毫秒为单位。将此选项设为非零的超时值时，
				// 在与此 Socket 关联的 InputStream 上调用 read() 将只阻塞此时间长度。如果超过超时值，
				// 将引发 java.net.SocketTimeoutException，虽然 Socket
				// 仍旧有效。选项必须在进入阻塞操作前被启用才能生效。
				// 超时值必须是 > 0 的数。超时值为 0 被解释为无穷大超时值。
				socket.setSoTimeout(soTimeout);
				outputStream = new RedisOutputStream(socket.getOutputStream());
				inputStream = new RedisInputStream(socket.getInputStream());
			}
			catch (IOException ex)
			{
				broken = true;
				throw new JedisConnectionException(ex);
			}
		}
	}

	@Override
	public void close()
	{
		disconnect();
	}

	public void disconnect()
	{
		if (isConnected())
		{
			try
			{
				outputStream.flush();
				socket.close();
			}
			catch (IOException ex)
			{
				broken = true;
				throw new JedisConnectionException(ex);
			}
			finally
			{
				IOUtils.closeQuietly(socket);
			}
		}
	}

	/**
	 * 检查此链接是否可用 <br>
	 * 1.socket对象是否null <br>
	 * 2.socket对象是否与一个网络地址正确的绑定<br>
	 * 3.socket是否已经被关闭<br>
	 * 4.socket是否是链接状态<br>
	 * 5.socket是否输入流已经关闭<br>
	 * 6.socket是否输出流已经关闭
	 */
	public boolean isConnected()
	{
		return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected() && !socket.isInputShutdown()
				&& !socket.isOutputShutdown();
	}

	/**
	 * 获取返回
	 * 
	 * @return
	 */
	public String getStatusCodeReply()
	{
		flush();
		pipelinedCommands--;
		final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
		if (null == resp)
		{
			return null;
		}
		else
		{
			return SafeEncoder.encode(resp);
		}
	}

	public String getBulkReply()
	{
		final byte[] result = getBinaryBulkReply();
		if (null != result)
		{
			return SafeEncoder.encode(result);
		}
		else
		{
			return null;
		}
	}

	/**
	 * 读取命令返回
	 * 
	 * @return
	 */
	public byte[] getBinaryBulkReply()
	{
		// 在读取结果前刷新输出缓存的所有内容
		flush();
		pipelinedCommands--;
		return (byte[]) readProtocolWithCheckingBroken();
	}

	public Long getIntegerReply()
	{
		flush();
		pipelinedCommands--;
		return (Long) readProtocolWithCheckingBroken();
	}

	public List<String> getMultiBulkReply()
	{
		return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
	}

	@SuppressWarnings("unchecked")
	public List<byte[]> getBinaryMultiBulkReply()
	{
		flush();
		pipelinedCommands--;
		return (List<byte[]>) readProtocolWithCheckingBroken();
	}

	public void resetPipelinedCount()
	{
		pipelinedCommands = 0;
	}

	@SuppressWarnings("unchecked")
	public List<Object> getRawObjectMultiBulkReply()
	{
		return (List<Object>) readProtocolWithCheckingBroken();
	}

	public List<Object> getObjectMultiBulkReply()
	{
		flush();
		pipelinedCommands--;
		return getRawObjectMultiBulkReply();
	}

	@SuppressWarnings("unchecked")
	public List<Long> getIntegerMultiBulkReply()
	{
		flush();
		pipelinedCommands--;
		return (List<Long>) readProtocolWithCheckingBroken();
	}

	public List<Object> getAll()
	{
		return getAll(0);
	}

	/**
	 * 将Connection中所有缓存的命令都发送出去<br>
	 * 并阻塞读取各个命令的返回值
	 * 
	 * @param except
	 *            不读取命令的的位置(在管道操作中该值是0,因为没有一条命令是多余不需要返回值的,在事务操作中，
	 *            最后一条exec是不需要返回值的)
	 * @return
	 */
	public List<Object> getAll(int except)
	{
		List<Object> all = new ArrayList<Object>();
		flush();
		while (pipelinedCommands > except)
		{
			try
			{
				all.add(readProtocolWithCheckingBroken());
			}
			catch (JedisDataException e)
			{
				all.add(e);
			}
			pipelinedCommands--;
		}
		return all;
	}

	public Object getOne()
	{
		flush();
		pipelinedCommands--;
		return readProtocolWithCheckingBroken();
	}

	public boolean isBroken()
	{
		return broken;
	}

	/**
	 * 在读取结果前刷新输出缓存的所有内容
	 */
	protected void flush()
	{
		try
		{
			outputStream.flush();
		}
		catch (IOException ex)
		{
			broken = true;
			throw new JedisConnectionException(ex);
		}
	}

	/**
	 * 发送命令后读取一条命令的返回值
	 * 
	 * @return
	 */
	protected Object readProtocolWithCheckingBroken()
	{
		try
		{
			return Protocol.read(inputStream);
		}
		catch (JedisConnectionException exc)
		{
			broken = true;
			throw exc;
		}
	}
}
