package com.asdust.cuckoofilter.utils;

import com.google.common.annotations.Beta;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author: Jianxing.Huang
 * @date: 2022/8/4 15:52
 * @desc:
 */
public class HashUtils {

    private static final int seedNSalt = 0;
    private static final HashFunction hasher = Hashing.murmur3_128(seedNSalt);

    public static long hash(String item) {
        Hasher hashInst = hasher.newHasher();
        hashInst.putString(item, StandardCharsets.UTF_8);
        hashInst.putLong(seedNSalt);
        return  hashInst.hash().asLong();
    }


    /**
     * The hashing algorithm used internally.
     *
     * @author Mark Gunlogson
     */
    public enum Algorithm {
        /**
         * Murmer3 - 32 bit version, This is the default.
         */
        Murmur3_32(0),
        /**
         * Murmer3 - 128 bit version. Slower than 32 bit Murmer3, not sure why
         * you would want to use this.
         */
        Murmur3_128(1),
        /**
         * SHA1 secure hash.
         */
        sha256(2),
        /**
         * SipHash(2,4) secure hash.
         */
        sipHash24(3),
        /**
         * xxHash 64bit.
         */
        xxHash64(4);
        private final int id;

        Algorithm(int id) {
            this.id = id;
        }

        public int getValue() {
            return id;
        }
    }
}
