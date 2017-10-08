import java.util.UUID;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class StringFactory implements PooledObjectFactory<String>
{
	public StringFactory()
	{
		System.out.println("init string factory..");
	}

	public void activateObject(PooledObject<String> pool) throws Exception
	{
		// TODO Auto-generated method stub

	}

	public void destroyObject(PooledObject<String> pool) throws Exception
	{
		String str = pool.getObject();
		if (str != null)
		{
			str = null;
			System.out.println(str + " destroy...");
		}
	}

	public PooledObject<String> makeObject() throws Exception
	{
		String i = UUID.randomUUID().toString();
		System.out.println("make " + i + " success...");
		return new DefaultPooledObject<String>(i);
	}

	public void passivateObject(PooledObject<String> pool) throws Exception
	{
		// TODO Auto-generated method stub

	}

	public boolean validateObject(PooledObject<String> pool)
	{
		// TODO Auto-generated method stub
		return false;
	}

}