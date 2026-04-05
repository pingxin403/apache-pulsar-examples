package com.example.pulsar;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.util.concurrent.TimeUnit;

/**
 * 基于 Pulsar 事务的支付服务
 * 
 * 演示跨账户转账场景：
 * 1. 从源账户扣款（debit-topic）
 * 2. 向目标账户入账（credit-topic）
 * 3. 记录交易日志（transaction-log-topic）
 * 
 * 三个操作在同一个 Pulsar 事务中完成，保证原子性。
 */
public class PaymentService {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String DEBIT_TOPIC = "persistent://public/default/debit-events";
    private static final String CREDIT_TOPIC = "persistent://public/default/credit-events";
    private static final String TX_LOG_TOPIC = "persistent://public/default/transaction-log";

    public static void main(String[] args) throws Exception {
        System.out.println("🏦 启动支付服务（Pulsar 事务模式）...\n");

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .enableTransaction(true)
                .build();

        Producer<String> debitProducer = client.newProducer(Schema.STRING)
                .topic(DEBIT_TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Producer<String> creditProducer = client.newProducer(Schema.STRING)
                .topic(CREDIT_TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Producer<String> logProducer = client.newProducer(Schema.STRING)
                .topic(TX_LOG_TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        // 模拟转账交易
        String[][] transfers = {
                {"ACC-001", "ACC-002", "100.00", "日常转账"},
                {"ACC-003", "ACC-001", "250.50", "工资发放"},
                {"ACC-002", "ACC-003", "75.00", "还款"},
        };

        for (String[] transfer : transfers) {
            String fromAccount = transfer[0];
            String toAccount = transfer[1];
            String amount = transfer[2];
            String memo = transfer[3];

            System.out.printf("💳 处理转账: %s → %s, 金额: ¥%s (%s)\n",
                    fromAccount, toAccount, amount, memo);

            try {
                // 开启事务
                Transaction txn = client.newTransaction()
                        .withTransactionTimeout(30, TimeUnit.SECONDS)
                        .build()
                        .get();

                // 1. 扣款
                String debitMsg = String.format(
                        "{\"account\":\"%s\",\"type\":\"debit\",\"amount\":-%s}",
                        fromAccount, amount);
                debitProducer.newMessage(txn).value(debitMsg).send();

                // 2. 入账
                String creditMsg = String.format(
                        "{\"account\":\"%s\",\"type\":\"credit\",\"amount\":%s}",
                        toAccount, amount);
                creditProducer.newMessage(txn).value(creditMsg).send();

                // 3. 交易日志
                String logMsg = String.format(
                        "{\"from\":\"%s\",\"to\":\"%s\",\"amount\":%s,\"memo\":\"%s\",\"status\":\"completed\"}",
                        fromAccount, toAccount, amount, memo);
                logProducer.newMessage(txn).value(logMsg).send();

                // 提交事务
                txn.commit().get();
                System.out.printf("   ✅ 事务提交成功\n\n");

            } catch (Exception e) {
                System.out.printf("   ❌ 事务失败: %s\n\n", e.getMessage());
            }
        }

        System.out.println("✅ 支付服务示例执行完成");
        System.out.println("💡 所有转账操作都在 Pulsar 事务中完成，保证原子性");

        debitProducer.close();
        creditProducer.close();
        logProducer.close();
        client.close();
    }
}
