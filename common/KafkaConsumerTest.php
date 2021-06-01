<?php

namespace Stary\Common\Tests\SDK;

use PHPUnit\Framework\TestCase;
use Stary\Common\MQ\Kafka\KafkaProducer;
use Stary\Common\MQ\Kafka\KafkaConsumer;

class KafkaConsumerTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testInstance()
    {
        $config = [
            'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 192.168.59.100:9095',
            'groupId'=>'group_1',
            'clientId'=>'',
            'rebalanceLogPath'=>'d:/rebalanceLog'
        ];
        $consumer = ( KafkaConsumer::instance('test', $config) );

        $message = new \RdKafka\Message();
        $message->err = 0;
        $message->topic_name = 'test';
        $message->timestamp = 1622546178700;
        $message->partition = 0;
        $message->payload = 'A message for test';
        $message->len = 18;
        $message->key = 373;
        $message->offset = 1;
        $message->headers = '';

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                var_dump($message);
                //消费完成后手动提交offset
                $consumer->commit($message);
                $this->assertNotEmpty($message);
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
}
