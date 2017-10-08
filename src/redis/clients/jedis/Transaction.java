package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Transaction is nearly identical to Pipeline, only differences are the
 * multi/discard behaviors
 * 
 * 事务操作与管道操作的不同点除了事务操作多了multi和discard命令之外<br>
 * 事务操作可以由watch的乐观锁控制，并且事务操作在redis中真正的命令执行是在发送exec之后，而管道则是立即执行的
 */
public class Transaction extends MultiKeyPipelineBase implements Closeable
{

	protected boolean inTransaction = true;

	protected Transaction()
	{
		// client will be set later in transaction block
	}

	public Transaction(final Client client)
	{
		this.client = client;
	}

	@Override
	protected Client getClient(String key)
	{
		return client;
	}

	@Override
	protected Client getClient(byte[] key)
	{
		return client;
	}

	public void clear()
	{
		if (inTransaction)
		{
			discard();
		}
	}

	/**
	 * 发送事务的exec命令同时获取命令的返回值除了exec命令的
	 * 
	 * @return
	 */
	public List<Object> exec()
	{
		client.exec();
		client.getAll(1); // Discard all but the last reply
		inTransaction = false;

		List<Object> unformatted = client.getObjectMultiBulkReply();
		if (unformatted == null)
		{
			return null;
		}
		List<Object> formatted = new ArrayList<Object>();
		for (Object o : unformatted)
		{
			try
			{
				formatted.add(generateResponse(o).get());
			}
			catch (JedisDataException e)
			{
				formatted.add(e);
			}
		}
		return formatted;
	}

	public List<Response<?>> execGetResponse()
	{
		client.exec();
		client.getAll(1); // Discard all but the last reply
		inTransaction = false;

		List<Object> unformatted = client.getObjectMultiBulkReply();
		if (unformatted == null)
		{
			return null;
		}
		List<Response<?>> response = new ArrayList<Response<?>>();
		for (Object o : unformatted)
		{
			response.add(generateResponse(o));
		}
		return response;
	}

	public String discard()
	{
		client.discard();
		client.getAll(1); // Discard all but the last reply
		inTransaction = false;
		clean();
		return client.getStatusCodeReply();
	}

	@Override
	public void close() throws IOException
	{
		clear();
	}
}