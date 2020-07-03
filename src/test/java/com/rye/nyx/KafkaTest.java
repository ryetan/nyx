package com.rye.nyx;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.rye.nyx.manager.KafkaManager;

@SpringBootTest
public class KafkaTest {

	@Test
	public void kafkaManagerTest() {
		KafkaManager kafkaManager = new KafkaManager();
		String topic = "cxstest";
		String message = "test message";
		// 生产消息
		for (int i = 1; i < 99; i++) {
			kafkaManager.sendMessage(topic, message + RandomStringUtils.randomAlphanumeric(5));
		}

		// 单线程消费
		kafkaManager.consumerMessage(topic);

	}
	
	

}
