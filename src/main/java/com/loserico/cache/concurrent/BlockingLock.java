package com.loserico.cache.concurrent;

import com.loserico.cache.JedisUtils;
import com.loserico.cache.listeners.MessageListener;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPubSub;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 阻塞锁
 * <p>
 * Copyright: (C), 2020/3/28 17:57
 * <p>
 * <p>
 * Company: Sexy Uncle Inc.
 *
 * @author Rico Yu ricoyu520@gmail.com
 * @version 1.0
 */
@Slf4j
public class BlockingLock implements Lock {
	
	private static final int NCPUS = Runtime.getRuntime().availableProcessors();
	
	/**
	 * The number of times to spin before blocking in timed waits.
	 * The value is empirically derived -- it works well across a
	 * variety of processors and OSes. Empirically, the best value
	 * seems not to vary with number of CPUs (beyond 2) so is just
	 * a constant.
	 */
	private static final int maxTimedSpins = (NCPUS < 2) ? 0 : 32;
	
	/**
	 * 锁的模板
	 */
	private static final String LOCK_FORMAT = "loser:blk:%s:lock";
	
	/**
	 * 解锁channel模板
	 */
	private static final String NOTIFY_CHANNEL_FORMAT = "loser:blk:%s:lock:channel";
	
	/**
	 * 解锁后在该channel上通知等待线程可以获取锁了
	 */
	private String notifyChannel;
	
	/**
	 * 分布式锁的key
	 */
	private String key;
	
	/**
	 * 锁的值, 解锁要用到
	 */
	private String value;
	
	/**
	 * 负责定时刷新锁过期时间
	 */
	private Timer watchDog = null;
	
	/**
	 * 当前线程自旋获取锁失败后, 会先订阅notifyChannel, 然后进入阻塞状态;
	 * 如果拿到锁的线程解锁, 会发布一条消息, 此时本线程被唤醒再次尝试获取锁
	 */
	private JedisPubSub subscribe = null;
	
	/**
	 * 锁默认30秒过期
	 */
	private int defaultTimeout = 30;
	
	public BlockingLock(String key) {
		this.key = String.format(LOCK_FORMAT, key);
		this.notifyChannel = String.format(NOTIFY_CHANNEL_FORMAT, key);
		this.value = UUID.randomUUID().toString();
	}
	
	/**
	 * 加锁, 锁有效期30秒
	 * 如果本线程被杀死, 30秒后自动释放锁
	 * 如果本线程一直在执行并且没有释放锁, 会有一条watchDog定时刷新锁的过期时间, 防止被其他线程获取锁
	 */
	@Override
	public void lock() {
		int i = 0;
		String threadName = Thread.currentThread().getName();
		
		/**
		 * 尝试第一次加锁, 加锁成功则启动watchDog并返回
		 */
		boolean locked = JedisUtils.setnx(key, value, 30, TimeUnit.SECONDS);
		if (locked) {
			log.info(">>>>>> {} 获取锁成功 <<<<<<", threadName);
			startWatchDog();
			return;
		}
		
		log.info(">>>>>> {} 第一次没能成功获取锁, 开始自旋获取锁 <<<<<<", threadName);
		/**
		 * 尝试maxTimedSpins次自旋获取锁, 加锁成功则启动watchDog并返回
		 */
		while (i++ < maxTimedSpins) {
			locked = JedisUtils.setnx(key, value, 30, TimeUnit.SECONDS);
			if (locked) {
				log.info(">>>>>> {} 自旋{}次获取锁成功 <<<<<<", threadName, i);
				startWatchDog();
				return;
			}
			log.info(">>>>>> {} 自旋{}次获取锁失败 <<<<<<", threadName, i);
		}
		
		log.info(">>>>>> {} 自旋失败, 进入阻塞等待 <<<<<<", threadName);
		/**
		 * 循环获取锁, 获取加锁成功则启动watchDog并返回
		 * 加锁失败挂起线程
		 */
		for (; ; ) {
			startListener();
			/**
			 * 阻塞30秒后自动醒来
			 * 期间如果Listener收到通知, 则提前唤醒本线程
			 * 如果获取锁的线程一直没有解锁, 或者那个线程被杀死了, 也就是锁一直没有被释放, 本线程过30秒也会自动醒来, 防止死锁
			 */
			LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(30));
			
			locked = JedisUtils.setnx(key, value, 30, TimeUnit.SECONDS);
			if (locked) {
				log.info(">>>>>> {} 醒来后终获成功 <<<<<<", threadName);
				stopListener();
				startWatchDog();
				return;
			}
			log.info("{} 醒来后仍然没有获取到锁, 准备再次进入阻塞状态", threadName);
		}
	}
	
	@Override
	public void unlock() {
		//解锁
		JedisUtils.unlock(key, value);
		/**
		 * 通知其他线程可以重新获取锁了, 吧当前线程名作为消息发出去, 方便记log
		 */
		JedisUtils.publish(notifyChannel, Thread.currentThread().getName());
		/**
		 * 关掉看门狗
		 */
		stopWatchDog();
	}
	
	@Override
	public boolean locked() {
		return false;
	}
	
	@Override
	public void unlockAnyway() {
		
	}
	
	/**
	 * 订阅通知channel, 只会订阅一次
	 */
	public void startListener() {
		if (this.subscribe == null) {
			this.subscribe = JedisUtils.subscribe(notifyChannel, new NotifyListener(Thread.currentThread()));
		}
	}
	
	public void stopListener() {
		if (this.subscribe != null) {
			JedisUtils.unsubscribe(subscribe, notifyChannel);
			this.subscribe = null;
		}
	}
	
	private class NotifyListener implements MessageListener {
		
		private Thread thread;
		
		public NotifyListener(Thread thread) {
			this.thread = thread;
		}
		
		@Override
		public void onMessage(String channel, String message) {
			log.info("收到 {} 发来的消息, 准备唤醒 {}", message, thread.getName());
			LockSupport.unpark(thread);
		}
	}
	
	/**
	 * 定时刷新锁的过期时间
	 */
	private void startWatchDog() {
		if (watchDog == null) {
			watchDog = new Timer("Loser Cache key renewval watch dog");
		}
		watchDog.schedule(new TimerTask() {
			@Override
			public void run() {
				JedisUtils.expire(key, defaultTimeout, TimeUnit.SECONDS);
			}
		}, 20L, 20);
	}
	
	private void stopWatchDog() {
		if (watchDog != null) {
			watchDog.cancel();
			watchDog = null;
		}
	}
}
