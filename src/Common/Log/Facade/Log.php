<?php
namespace StaryPHP\Common\Log\Facade;

use StaryPHP\Common\MQ\Kafka\NmredKafkaProducer;
use StaryPHP\Common\MQ\Kafka\NmredKafkaSyncProducer;
use StaryPHP\Common\MQ\Kafka\KafkaProducer;
use StaryPHP\Common\MQ\Kafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

/**
 * @description MQ门面类（facade类）
 * @class MQ
 */
class Log
{

    private static $config = [
        /*设置默认走nmred-sync-kafka*/
        'type'=>'nmred-sync-kafka'
    ];

    /**
     * 消息生产门面
     * @param string $topic 主题
     * @param string $message 消息内容
     * @param array $config 配置
     * @return boolean
     * @throws \Exception
     */
    public static function instance($topic, $message, $config)
    {

        $config = array_merge(self::$config, $config);

        switch ($config['type']) {
            /* self::$config 中设置了默认走nmred-sync-kafka */
            //nmred-sync-kafka (nmred/kafka的同步模式)
            case 'nmred-sync-kafka':
                return ( NmredKafkaSyncProducer::instance($topic, $config) )->produce($message);
                break;
            // (nmred/kafka的异步模式)
            case 'nmred-kafka':
                return ( NmredKafkaProducer::instance($topic, $config) )->produce($message);
                break;
            /* 如果是rdkafka */
            case 'kafka':
                return ( KafkaProducer::instance($topic, $config) )->produce($message);
                break;
            default:
                break;
        }

        return false;
    }

    /**
     * 消息生产门面
     * @param string $topic 主题
     * @param string $message 消息内容
     * @param array $config 配置
     * @return boolean
     * @throws \Exception
     */
    public static function produce($topic, $message, $config)
    {
        echo 99;exit;

        return false;
    }

    /**
     * 消息消费门面
     * @param string $topic 主题
     * @param object $callback 处理消息的类和方法
     * @param array $config 配置
     * @return void
     * @throws \Exception
     */
    public static function consum($topic, $callback, $config)
    {

        $config = array_merge(self::$config, $config);

        switch ($config['type']) {
            /* self::$config 中设置了默认走kafka */
            case 'kafka':
                return ( KafkaConsumer::instance($topic, $config) )
                    ->callback($callback);
                break;
            default:
                break;
        }
    }
}
