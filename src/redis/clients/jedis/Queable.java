package redis.clients.jedis;

import java.util.LinkedList;
import java.util.Queue;

/**
 * PipelineBase �ܵ���������ĸ��࣬���ڴ洢ÿ���ܵ�����ķ�����������<br>
 * һ�����У��Ƚ��ȳ�(����ӽ����������ڶ�ȡ����ֵʱ˳�����)
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
