<?php

require_once '../src/Common/MQ/Kafka/KafkaProducer.php';

$config = [
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
    'clientId'=>'',
    'drCbLogPath'=>'d:/kafkaProducerDrLog',
    'errCbLogPath'=>'d:/kafkaProducerErrLog',
    'ack'=>1
];

( \StaryPHP\Common\MQ\Kafka\KafkaProducer::instance('test', $config) )->produce(['A message for test']);