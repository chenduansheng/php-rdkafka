<?php
namespace Stary\Common\MQ\Facade;

use Stary\Common\MQ\Kafka\KafkaProducer;
use Stary\Common\MQ\Kafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

/**
 * @description MQ门面类（facade类）
 * @class MQ
 */
class MQ
{

    private static $config = [
        /*设置默认走kafka*/
        'type'=>'kafka'
    ];

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

        $config = array_merge(self::$config, $config);

        switch ($config['type']) {
            /* self::$config 中设置了默认走kafka */
            case 'kafka':
                return ( KafkaProducer::instance($topic, $config) )->produce($message);
                break;
            default:
                break;
        }

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
