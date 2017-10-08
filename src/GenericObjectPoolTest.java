import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class GenericObjectPoolTest
{
	public static void main(String[] args)
	{
		GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
		conf.setMaxTotal(10);
		GenericObjectPool<String> pool = new GenericObjectPool<String>(new StringFactory(), conf);
		for (int i = 0; i < 15; i++)
		{
			System.out.println(i + ":");
			try
			{
				String str = pool.borrowObject();
				System.out.println(str);
				pool.returnObject(str);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
