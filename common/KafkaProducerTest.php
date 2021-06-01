<?php

namespace Stary\Common\Tests\SDK;

use PHPUnit\Framework\TestCase;
use Stary\Common\MQ\Kafka\KafkaProducer;

class KafkaProducerTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testSend()
    {
        $config = [
            'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
            'clientId'=>'',
            'drCbLogPath'=>'d:/kafkaProducerDrLog',
            'errCbLogPath'=>'d:/kafkaProducerErrLog',
            'ack'=>1
        ];

        $result = ( \Stary\Common\MQ\Kafka\KafkaProducer::instance('test', $config) )->send('A message for test');

        $this->assertEquals(true, $result);
    }
}
