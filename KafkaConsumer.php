<?php

namespace Common\MQ\Kafka;

use Exception;
use RdKafka\KafkaConsumer as RkConsumer;
use RdKafka\Message;
use RdKafka\Conf;

/**
 * @description kafka消息消费者类包, 使用前需安装rdkafka拓展
 * @class KafkaConsumer
 */
class KafkaConsumer
{
    /**
     * 单例模式
     * @var KafkaConsumer
     */
    private static $instance = [];
    /**
     * kafka消费实例
     * @var RkConsumer
     */
    public $consumer;
    /**
     * topic主题名
     * @var string
     */
    private $topic;

    private $config = [
        'brokerList'=>'127.0.0.1:9092', //连接kafka的broker
        'groupId'=>'', //设置分组id
        'clientId'=>'', //设置客户端
        'rebalanceLogPath'=>'/var/log/KafkaRebalanceLog'
    ];

    /**
     * @param string $topic
     * @param array  $topicConfig
     * @return KafkaConsumer
     * @throws \RdKafka\Exception
     */
    public static function instance(string $topic, array $config): object
    {
        if (!isset(self::$instance[$topic])) {
            self::$instance[$topic] = new self($topic, $config);
        }

        return (self::$instance[$topic])->consumer;
    }


    /**
     * @throws \RdKafka\Exception
     */
    public function __construct(string $topic, array $config)
    {
        $this->config = array_merge($this->config, $config);
        $this->topic = $topic;

        /*实例化kafak配置*/
        $conf = new Conf();

        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb([$this, 'kafkaRebalanceCb']);

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', $this->config['groupId']);

        if(!empty($this->config['clientId'])){
            $conf->set('client.id', $this->config['clientId']);
        }

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', $this->config['brokerList']);

        //设置使用手动提交offset
        $conf->set('enable.auto.commit', 'false');

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', 'smallest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        // Subscribe to topic 'test'
        $consumer->subscribe([$this->topic]);

        $this->consumer = $consumer;
    }

    /**
     * @throws Exception
     */
    public function kafkaRebalanceCb(RkConsumer $kafka, $err, array $partitions = null)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                echo "Assign: ";
                var_dump($partitions);
                $this->log('Assign:', $this->config['rebalanceLogPath']);
                $this->log(var_export($partitions, true), $this->config['rebalanceLogPath']);
                $kafka->assign($partitions);
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                echo "Revoke: ";
                var_dump($partitions);
                $this->log('Revoke:', $this->config['rebalanceLogPath']);
                $this->log(var_export($partitions, true), $this->config['rebalanceLogPath']);
                $kafka->assign();
                break;

            default:
                throw new Exception($err);
        }
    }

    /**
     * @param string $message 日志内容
     * @param string $path    日志路径
     * @return void
     */
    private function log($message, $path){
        $dateTime = date('Y-m-d H:i:s');

        $date = date('Y-m-d', time());
        $path = $path.'.'.$date.'.log';

        error_log("[$dateTime] $message".PHP_EOL, 3, $path);
    }
}
