<?php

require_once '../src/Common/MQ/Kafka/NmredKafkaProducer.php';
require_once '../nmred/kafka-php/src/Kafka/ProducerConfig.php';

$config = [
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
    'clientId'=>'',
    'drCbLogPath'=>'d:/kafkaProducerDrLog',
    'errCbLogPath'=>'d:/kafkaProducerErrLog',
    'ack'=>1
];

( \StaryPHP\Common\MQ\Kafka\NmredKafkaProducer::instance('test2', $config) )->send('A message for test');