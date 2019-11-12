package com.loserico.cache.concurrent;

import com.loserico.cache.JedisUtils;
import com.loserico.cache.exception.OperationNotSupportedException;
import com.loserico.cache.utils.DateUtils;
import com.loserico.common.lang.utils.ReflectionUtils;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * 非阻塞锁
 * 这个锁是线程独有的, 不要作为共享对象
 * <p>
 * Copyright: Copyright (c) 2018-07-16 10:32
 * <p>
 * Company: DataSense
 * <p>
 *
 * @author Rico Yu	ricoyu520@gmail.com
 * @version 1.0
 */
public class NonBlockingLock implements Lock, Expirable {

	/**
	 * 这是commons-spring模块中的类
	 */
	private static final String TRANSACTION_EVENTS_CLASS_NAME = "com.loserico.common.spring.transaction.TransactionEvents";

	private String key;

	private String requestId;

	private boolean locked;

	/**
	 * transactionEventsInstance是否已经通过反射获取过
	 */
	private boolean transactionEventsInitialized = false;
	private Object transactionEventsInstance;

	public NonBlockingLock(String key, String requestId, boolean locked) {
		this.key = key;
		if (locked) {
			this.requestId = requestId;
		}
		this.locked = locked;
	}

	@Override
	public void unlock() {
		if (locked) {
			boolean unlockSuccess = JedisUtils.unlock(key, requestId);
			if (!unlockSuccess) {
				throw new OperationNotSupportedException("解锁失败了哟");
			}
		} else {
			throw new OperationNotSupportedException("你还没获取到锁哦");
		}
	}

	@Override
	public void unlockAnyway() {
		if (this.transactionEventsInitialized) {
			throw new OperationNotSupportedException("unlockAnyway()只能调一次");
		}
		if (locked) {
			//调用这个类的instance()方法, 如果这个类不在classpath下会直接抛异常提示找不到这个类, 所以这里不抛异常肯定能找到一个对象
			transactionEventsInstance = ReflectionUtils.invokeStatic(TRANSACTION_EVENTS_CLASS_NAME, "instance");
			this.transactionEventsInitialized = true;
			ReflectionUtils.invokeMethod(transactionEventsInstance, "afterCompletion", () -> JedisUtils.unlock(key, requestId));
		} else {
			throw new OperationNotSupportedException("你还没获取到锁哦");
		}
	}

	@Override
	public boolean locked() {
		return locked;
	}

	@Override
	public boolean expire(long timeToLive, TimeUnit timeUnit) {
		if (locked) {
			return JedisUtils.expire(key, ((Long) timeToLive).intValue(), timeUnit);
		}
		throw new OperationNotSupportedException("你还没获取到锁哦");
	}

	@Override
	public boolean expireAt(long timestamp) {
		return JedisUtils.expireAt(key, timestamp);
	}

	@Override
	public boolean expireAt(LocalDateTime timestamp) {
		if (timestamp == null) {
			return false;
		}
		return JedisUtils.expireAt(key, DateUtils.toEpochMilis(timestamp));
	}

	@Override
	public boolean clearExpire() {
		return JedisUtils.persist(key);
	}

	@Override
	public long remainTimeToLive() {
		return JedisUtils.ttl(key);
	}

	/**
	 * 不支持该操作, 请改用RedissonLock
	 * throws UnsupportedOperationException
	 */
	@Override
	public void lock() {
		throw new UnsupportedOperationException("NonBlockingLock not support lock(), use RedissonLock instead");
	}

	/**
	 * 不支持该操作, 请改用RedissonLock
	 * throws UnsupportedOperationException
	 */
	@Override
	public void lockInterruptibly() {
		throw new UnsupportedOperationException("NonBlockingLock not support lockInterruptibly(), use RedissonLock instead");
	}

	/**
	 * 不支持该操作, 请改用RedissonLock
	 * throws UnsupportedOperationException
	 */
	@Override
	public boolean tryLock() {
		throw new UnsupportedOperationException("NonBlockingLock not support tryLock(), use RedissonLock instead");
	}

	/**
	 * 不支持该操作, 请改用RedissonLock
	 * throws UnsupportedOperationException
	 */
	@Override
	public boolean tryLock(long time, TimeUnit unit) {
		throw new UnsupportedOperationException("NonBlockingLock not support tryLock(time, unit), use RedissonLock instead");
	}

	/**
	 * 不支持该操作, 请改用RedissonLock
	 * throws UnsupportedOperationException
	 */
	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException("NonBlockingLock not support newCondition(), use RedissonLock instead");
	}

}
