<?php

require_once '../KafkaConsumer.php';


$config = [
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 192.168.59.100:9095',
    'groupId'=>'group_1',
    'clientId'=>'',
    'rebalanceLogPath'=>'d:/44444'
];
$consumer = ( \Common\MQ\Kafka\KafkaConsumer::instance('test', $config) );

while (true) {
    $message = $consumer->consume(120 * 1000);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            var_dump($message);
            //消费完成后手动提交offset
            $consumer->commit($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            echo "No more messages; will wait for more\n";
            break;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Timed out\n";
            break;
        default:
            throw new \Exception($message->errstr(), $message->err);
            break;
    }
}