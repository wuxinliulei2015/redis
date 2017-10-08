package redis.clients.util;

import java.io.UnsupportedEncodingException;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * The only reason to have this is to be able to compatible with java 1.5 :(
 */
public class SafeEncoder
{
	/**
	 * 返回多个string参数的字节数组形式
	 * 
	 * @param strs
	 *            多个string参数
	 * @return 返回字节二维数组
	 */
	public static byte[][] encodeMany(final String... strs)
	{
		byte[][] many = new byte[strs.length][];
		for (int i = 0; i < strs.length; i++)
		{
			many[i] = encode(strs[i]);
		}
		return many;
	}

	/**
	 * 返回String参数的字节数组,默认使用UTF-8编码
	 * 
	 * @param str
	 *            字符串参数
	 * @return 字节数组
	 */
	public static byte[] encode(final String str)
	{
		try
		{
			if (str == null)
			{
				throw new JedisDataException("value sent to redis cannot be null");
			}
			return str.getBytes(Protocol.CHARSET);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new JedisException(e);
		}
	}

	public static String encode(final byte[] data)
	{
		try
		{
			return new String(data, Protocol.CHARSET);
		}
		catch (UnsupportedEncodingException e)
		{
			throw new JedisException(e);
		}
	}
}
