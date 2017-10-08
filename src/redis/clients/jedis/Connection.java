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
 * jedis��redis����<B>TCPͨ��</B>��������࣬����TCP���ӵ�ʵʩ��
 * <p>
 */
public class Connection implements Closeable
{

	private static final byte[][] EMPTY_ARGS = new byte[0][];

	/**
	 * Ĭ�����ӵ�ip:localhost
	 */
	private String host = Protocol.DEFAULT_HOST;
	/**
	 * Ĭ�϶˿ںţ�6379
	 */
	private int port = Protocol.DEFAULT_PORT;

	/**
	 * TCP���ӵ�socket����
	 */
	private Socket socket;

	/**
	 * ��TCP���ӵ����������OutputStream��װ������
	 */
	private RedisOutputStream outputStream;

	/**
	 * ��TCP���ӵ�����������InputStream��װ������
	 */
	private RedisInputStream inputStream;

	/**
	 * ÿ����һ�������ֵ��1 ÿ�յ�һ�����أ���ֵ��1
	 */
	private int pipelinedCommands = 0;

	/**
	 * Ĭ�ϳ�ʱʱ�䣺2000
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
	 * �����ø����ӷ���redis��������ǰ��������ã�
	 * <p>
	 * �����ڽ������ӣ�
	 * <p>
	 * ���������Ƿ���Ȼ���ã������������½�������
	 */
	public void connect()
	{
		if (!isConnected())
		{
			try
			{
				socket = new Socket();

				// ����/���� SO_REUSEADDR �׽���ѡ�
				// �ر� TCP ����ʱ�������ӿ����ڹرպ��һ��ʱ���ڱ��ֳ�ʱ״̬��ͨ����Ϊ TIME_WAIT ״̬�� 2MSL
				// �ȴ�״̬��������ʹ����֪�׽��ֵ�ַ��˿ڵ�Ӧ�ó�����ԣ�������ڴ��ڳ�ʱ״̬�����ӣ�������ַ�Ͷ˿ڣ���
				// ���ܲ��ܽ��׽��ְ󶨵������SocketAddress �ϡ�
				// ʹ��bind(SocketAddress),���׽���ǰ����,SO_REUSEADDR,��������һ�����Ӵ��ڳ�ʱ״̬ʱ���׽��֡�
				// ������ Socket ʱ������ֵ SO_REUSEADDR �ĳ�ʼ������Ĭ�Ϲرյ�;�ڰ��׽��ֺ����û����
				// SO_REUSEADDR ʱ�Ľ���ǲ�ȷ����,������Ҫ�ڰ��׽���֮ǰ���ø�ֵ��
				socket.setReuseAddress(true);

				// keepalive����˵TCP�ĳ����ӣ���������Ϊ����ˣ�һ���ͻ����������������������keepliveΪtrue��
				// ���Է�û�з����κ����ݹ���������һ��ʱ��(��ϵͳ�ں˲�������)����ô������߻ᷢ��һ��ack̽��������Է���
				// ̽��˫����TCP/IP�����Ƿ���Ч(�Է����ܶϵ㣬����)����������ã���ô�ͻ���崻�ʱ����������ԶҲ��֪���ͻ���崻��ˣ�
				// ��Ȼ�������ʧЧ�����ӡ�
				// ��Ȼ���ڿͻ���Ҳ����ʹ������������ͻ���Socket��ÿ���ε�ʱ�䣨��Լ����Сʱ���ͻ����ÿ��е����������������һ�����ݰ���
				// ������ݰ���û�����������ã�ֻ��Ϊ�˼��һ�·������Ƿ��Դ��ڻ״̬�����������δ��Ӧ������ݰ����ڴ�Լ11���Ӻ�
				// �ͻ���Socket�ٷ���һ�����ݰ��������12�����ڣ���������û��Ӧ����ô�ͻ���Socket���رա�
				// �������Socketѡ��رգ��ͻ���Socket�ڷ�������Ч������¿��᳤ܻʱ�䲻��رա�
				// ����keepalive�ĺô������࣬���Ǻܶ࿪�����ᳫ�ڸ��߲�ε�Ӧ�ó�������п��Ƴ�ʱ���ú������׽��֡�ͬʱ��Ҫ��ס��
				// keepalive��������Ϊ̽���׽����յ㣨endpoint��ָ��һ��ֵ�����Խ��鿪����ʹ�õ���һ�ֱ�keepalive���õĽ���������޸ĳ�ʱ�����׽���ѡ�
				// ˵���ˣ����������ʵ��Ӧ�ò�ĳ������û��ʲô�á�����ͨ��Ӧ�ò�ʵ���˽����˻�ͻ���״̬���������Ƿ����ά�ָ�Socket��
				// Will monitor the TCP connection is valid
				socket.setKeepAlive(true);

				// Socket buffer Whetherclosed, to ensure timely delivery of
				// data
				socket.setTcpNoDelay(true);

				// Control calls close () method,the underlying socket is closed
				// immediately
				socket.setSoLinger(true, 0);

				socket.connect(new InetSocketAddress(host, port), connectionTimeout);

				// ����/���ô���ָ����ʱֵ�� SO_TIMEOUT���Ժ���Ϊ��λ������ѡ����Ϊ����ĳ�ʱֵʱ��
				// ����� Socket ������ InputStream �ϵ��� read() ��ֻ������ʱ�䳤�ȡ����������ʱֵ��
				// ������ java.net.SocketTimeoutException����Ȼ Socket
				// �Ծ���Ч��ѡ������ڽ�����������ǰ�����ò�����Ч��
				// ��ʱֵ������ > 0 ��������ʱֵΪ 0 ������Ϊ�����ʱֵ��
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
	 * ���������Ƿ���� <br>
	 * 1.socket�����Ƿ�null <br>
	 * 2.socket�����Ƿ���һ�������ַ��ȷ�İ�<br>
	 * 3.socket�Ƿ��Ѿ����ر�<br>
	 * 4.socket�Ƿ�������״̬<br>
	 * 5.socket�Ƿ��������Ѿ��ر�<br>
	 * 6.socket�Ƿ�������Ѿ��ر�
	 */
	public boolean isConnected()
	{
		return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected() && !socket.isInputShutdown()
				&& !socket.isOutputShutdown();
	}

	/**
	 * ��ȡ����
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
	 * ��ȡ�����
	 * 
	 * @return
	 */
	public byte[] getBinaryBulkReply()
	{
		// �ڶ�ȡ���ǰˢ������������������
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
	 * ��Connection�����л����������ͳ�ȥ<br>
	 * ��������ȡ��������ķ���ֵ
	 * 
	 * @param except
	 *            ����ȡ����ĵ�λ��(�ڹܵ������и�ֵ��0,��Ϊû��һ�������Ƕ��಻��Ҫ����ֵ��,����������У�
	 *            ���һ��exec�ǲ���Ҫ����ֵ��)
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
	 * �ڶ�ȡ���ǰˢ������������������
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
	 * ����������ȡһ������ķ���ֵ
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
