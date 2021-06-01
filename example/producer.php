<?php

require_once '../KafkaProducer.php';

$config = [
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
    'clientId'=>'',
    'drCbLogPath'=>'d:/kafkaProducerDrLog',
    'errCbLogPath'=>'d:/kafkaProducerErrLog',
    'ack'=>1
];

( \Common\MQ\Kafka\KafkaProducer::instance('test', $config) )->send('A message for test');