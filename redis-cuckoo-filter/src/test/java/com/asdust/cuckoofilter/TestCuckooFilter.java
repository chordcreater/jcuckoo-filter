/*
   Copyright 2016 Mark Gunlogson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.asdust.cuckoofilter;

import com.asdust.cuckoofilter.redis.RedisConfig;
import com.google.common.hash.Funnels;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestCuckooFilter {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgsTooHighFp() {
		RedisConfig redisConfig = new RedisConfig();
		CuckooFilter<String>  cuckooFilter = new CuckooFilter<String> (2000000L, redisConfig);
		cuckooFilter.put("aaaa");
		assertTrue(cuckooFilter.contain("aaaa"));
		if(cuckooFilter.contain("aaaa")){
			cuckooFilter.delete("aaaa");
		}
		assertFalse(cuckooFilter.contain("aaaa"));
		// todo 后面还需要测试过滤的准确率，若测试可容纳10000元素的过滤器，往里面插入10000个元素，有3个是重复的，准确率=1-(3/10000)
	}



}
