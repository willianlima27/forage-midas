package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private final RestTemplate restTemplate;
    private static final String INCENTIVE_URL = "http://localhost:8080/incentive";

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_URL, transaction, Incentive.class);
            logger.info("Incentive for transaction {} is {}", transaction, incentive);
            return incentive;
        } catch (Exception e) {
            logger.error("Failed to get incentive for transaction {}", transaction, e);
            return new Incentive(0);
        }
    }
}
