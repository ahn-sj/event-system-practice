package com.example.api.service;

import com.example.api.repository.CouponRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ApplyServiceTest {

    @Autowired
    private ApplyService applyService;

    @Autowired
    private CouponRepository couponRepository;

    @Test
    void apply_one() {
        applyService.applyV1(1L);

        long count = couponRepository.count();

        assertThat(count).isEqualTo(1);
    }

    @Test
    void apply_multi_race_condition_O() throws InterruptedException {
        int threadCount = 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(32); // 고정된 개수의 쓰레드 풀 생성
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            long userId = i;

            // 멀티 쓰레드로 작업
            executorService.submit(() -> {
                try {
                    applyService.applyV1(userId);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        long count = couponRepository.count();
        assertThat(count).isEqualTo(100);
    }

    @Test
    void apply_multi_redis_incr_race_condition_X() throws InterruptedException {
        int threadCount = 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(32); // 고정된 개수의 쓰레드 풀 생성
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            long userId = i;

            // 멀티 쓰레드로 작업
            executorService.submit(() -> {
                try {
                    applyService.applyV2(userId);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        long count = couponRepository.count();
        assertThat(count).isEqualTo(100);
    }

    @Test
    void apply_multi_redis_incr_race_and_kafka_condition_X() throws InterruptedException {
        int threadCount = 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(32); // 고정된 개수의 쓰레드 풀 생성
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            long userId = i;

            // 멀티 쓰레드로 작업
            executorService.submit(() -> {
                try {
                    applyService.applyV3(userId);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        Thread.sleep(10000);

        long count = couponRepository.count();
        assertThat(count).isEqualTo(100);
    }

    @Test
    @DisplayName("1000번을 요청보내도 동일한 사용자에게는 1개의 쿠폰만 발급된다.")
    void apply_multi_redis_and_kafka_with_condition_and_one_by_one() throws InterruptedException {
        int threadCount = 1000;

        ExecutorService executorService = Executors.newFixedThreadPool(32); // 고정된 개수의 쓰레드 풀 생성
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    applyService.applyV4(1L);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        Thread.sleep(10000);

        long count = couponRepository.count();
        assertThat(count).isEqualTo(1);
    }
}