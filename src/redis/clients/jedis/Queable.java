package redis.clients.jedis;

import java.util.LinkedList;
import java.util.Queue;

/**
 * PipelineBase 管道基础对象的父类，用于存储每个管道命令的返回数据类型<br>
 * 一个队列，先进先出(先添加进来的命令在读取返回值时顺序读出)
 */
public class Queable
{
	private Queue<Response<?>> pipelinedResponses = new LinkedList<Response<?>>();

	protected void clean()
	{
		pipelinedResponses.clear();
	}

	protected Response<?> generateResponse(Object data)
	{
		Response<?> response = pipelinedResponses.poll();
		if (response != null)
		{
			response.set(data);
		}
		return response;
	}

	protected <T> Response<T> getResponse(Builder<T> builder)
	{
		Response<T> lr = new Response<T>(builder);
		pipelinedResponses.add(lr);
		return lr;
	}

	protected boolean hasPipelinedResponse()
	{
		return pipelinedResponses.size() > 0;
	}

	protected int getPipelinedResponseLength()
	{
		return pipelinedResponses.size();
	}
}
