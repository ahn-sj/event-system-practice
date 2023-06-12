package com.example.api.service;

import com.example.api.domain.Coupon;
import com.example.api.producer.CouponCreateProducer;
import com.example.api.repository.AppliedUserRepository;
import com.example.api.repository.CouponCountRepository;
import com.example.api.repository.CouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ApplyService {

    private final CouponRepository couponRepository;
    private final CouponCountRepository couponCountRepository;

    private final CouponCreateProducer couponCreateProducer;

    private final AppliedUserRepository appliedUserRepository;

    /**
     * 쿠폰 발급
     * @param userId
     */
    public void applyV1(Long userId) {
        long count = couponRepository.count();

        if(count > 100) {
            return;
        }
        couponRepository.save(new Coupon(userId));
    }

    /**
     * 쿠폰 발급 (Redis > incr coupon_count)
     * @param userId
     */
    public void applyV2(Long userId) {
        long count = couponCountRepository.increment();

        if(count > 100) {
            return;
        }
        couponRepository.save(new Coupon(userId));
    }

    /**
     * 쿠폰 발급 (Redis > incr coupon_count + kafka)
     */
    public void applyV3(Long userId) {
        long count = couponCountRepository.increment();

        if(count > 100) {
            return;
        }

        couponCreateProducer.create(userId);
    }

    /**
     * 쿠폰 발급 (Redis > incr coupon_count + kafka + Redis Set)
     * 조건: 1인 1쿠폰
     */
    public void applyV4(Long userId) {
        Long isIssued = appliedUserRepository.addByRedisSet(userId);

        if(isIssued != 1) {
            return;
        }

        long count = couponCountRepository.increment();

        if(count > 100) {
            return;
        }

        couponCreateProducer.create(userId);
    }
}
