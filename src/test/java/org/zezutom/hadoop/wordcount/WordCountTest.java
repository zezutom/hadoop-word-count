package org.zezutom.hadoop.wordcount;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = WordCount.class)
public class WordCountTest {

	@Test
	public void contextLoads() {
	}

}
