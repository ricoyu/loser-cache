package org.loser.cache;

import com.loserico.cache.JedisUtils;
import com.loserico.cache.concurrent.Lock;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Copyright: Copyright (c) 2019/10/18 10:46
 * <p>
 * Company: Sexy Uncle Inc.
 * <p>
 *
 * @author Rico Yu  ricoyu520@gmail.com
 * @version 1.0
 */
public class JedisSentinelTest {

	@Test
	public void testSetGet() {
		JedisUtils.set("rico", "很帅");
		String desc = JedisUtils.get("rico");
		System.out.println(desc);
	}
	@Test
	public void testNonBlockLock() {
		Lock lock = JedisUtils.lock("lock1", 1000, TimeUnit.SECONDS);
		lock.unlockAnyway();
	}
	
	@Test
	public void testGetMasterAddress() {
		Object address = JedisUtils.execute((jedis) -> {
			return jedis.sentinelGetMasterAddrByName("mymaster");
		});
		System.out.println(address);
	}
}
