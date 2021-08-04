<?php
namespace StaryPHP\Common\MQ\Kafka;

use Exception;
use Kafka\Producer;
use Kafka\ProducerConfig;


/**
 * @description kafka消息生产者类包, 使用前需安装rdkafka拓展
 * @class KafkaProducer
 */
class NmredKafkaProducer
{
    /**
     * 默认配置项，应用传值过来了就会用传过来的，其中brokerList为必传项
     * @var string
     */
    private $config = [
        'brokerList'=>'',                       //kafka地址和端口 - 【只有此项是必传】
        //'clientId'=>'',                         //客户端分配id
        //'drCbLogPath'=>'/var/log/kafka_dr_cb',  //回调函数保存回调日志的日志文件地址
        //'errCbLogPath'=>'/var/log/kafka_err_cb',//回调函数保存错误日志的日志文件地址
        'defaultLogPath'=>'/var/log/default',   //默认日志路径
        'flushToDisk'=>false,                   //是否将当前消息实时持久化到磁盘
        'ack'=>1,                                //是否需要给回调函数返回offset
        'BrokerVersion'=>'1.0.0',
        'isAsync'=>false
    ];

    /**
     * 生产者配置
     * @var object
     */
    private $producerConfig;

    /**
     * topic单例
     * @var object
     */
    private static $instance = [];

    /**
     * 生产者实例
     * @var object
     */
    public $producer;

    /**
     * kafka topic实例
     * @var object
     */
    public $topic;

    /**
     * kafka topic名称
     * @var object
     */
    public $topicName;

    /**
     * 单例方法，生成基于主题的单例生产者
     * @param string $topic
     * @param array $config
     * @return KafkaProducer
     */
    public static function instance(string $topic, array $appConfig): NmredKafkaProducer
    {
        if (!isset(self::$instance[$topic])) {
            self::$instance[$topic] = new self($topic, $appConfig);
        }

        return self::$instance[$topic];
    }


    /**
     * 会生成相关生产者实例
     * @param string $topic
     * @param array $config
     * @return void
     */
    public function __construct(string $topic, array $config)
    {
        $this->topicName = $topic;
        $this->config = array_merge($this->config, $config);

        $this->producerConfig = \Kafka\ProducerConfig::getInstance();

        if (!empty($this->config['brokerList'])) {
            $this->producerConfig->setMetadataBrokerList($this->config['brokerList']);
        } else {
            throw new \Exception('brokerList参数(kafka的ip和端口)没有配置');
        }

        if (isset($this->config['ack'])) {
            $this->producerConfig->setRequiredAck($this->config['ack']);
        }

        if (isset($this->config['isAsync'])) {
            $this->producerConfig->setIsAsyn($this->config['isAsync']);
        }

        if (isset($this->config['ProduceInterval'])) {
            $this->producerConfig->setProduceInterval($this->config['ProduceInterval']);
        }

        if (isset($this->config['BrokerVersion'])) {
            $this->producerConfig->setBrokerVersion($this->config['BrokerVersion']);
        }

        if (isset($this->config['MetadataRefreshIntervalMs'])) {
            $this->producerConfig->setMetadataRefreshIntervalMs($this->config['MetadataRefreshIntervalMs']);
        }
    }

    /**
     * 发送消息（基于主题的消息推送）
     * @param mixed $message
     * @return void
     */
    public function produce($message)
    {
        if (!$message) {
            throw new \Exception('$message：内容不能为空');
        }
        if (is_array($message)) {
            $message = json_encode($message, true);
        }
        if (!is_string($message)) {
            throw new \Exception('$message：参数格式错误');
        }

        $topicName = $this->topicName;
        $producer = new \Kafka\Producer(function() use ($message,$topicName) {
            return [
                [
                    'topic' => $topicName,
                    'value' => $message,
                ],
            ];
        });

        // 所有推送的回调地址
        if (isset($this->config['callback'])) {
            $producer->success($this->config['callback']);
        } elseif (isset($this->config['drCbLogPath'])) {
            $producer->success(function($result) {
                //回调日志
                $this->log(json_encode($result, true), $this->config['drCbLogPath']);
            });
        }

        // 所有错误推送的回调日志地址，该日志可追踪错误的推送
        if (isset($this->config['errCbLogPath'])) {
            $producer->error(function($errorCode) {
                $this->log($errorCode, $this->config['errCbLogPath']);
            });
        }

        $producer->send(true);

        return true;
    }

    /**
     * 保存相关日志
     * @param string $message 日志内容
     * @param string $path    日志路径
     * @return void
     */
    private function log($message, $path)
    {
        if (!$path) {
            $path = $this->config['defaultLogPath'];
        }
        $dateTime = date('Y-m-d H:i:s');

        $date = date('Ymd', time());

        $path = $path.'.'.$this->topicName.'.producer.kafka.log.'.$date;

        error_log("[$dateTime] $message".PHP_EOL, 3, $path);
    }
}
