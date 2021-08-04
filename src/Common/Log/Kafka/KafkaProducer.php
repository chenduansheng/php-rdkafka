<?php
namespace StaryPHP\Common\MQ\Kafka;

use Exception;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

/**
 * @description kafka消息生产者类包, 使用前需安装rdkafka拓展
 * @class KafkaProducer
 */
class KafkaProducer
{
    /**
     * 默认配置项，应用传值过来了就会用传过来的，其中brokerList为必传项
     * @var string
     */
    private $config = [
        'brokerList'=>'',                       //kafka地址和端口 - 【只有此项是必传】
        'clientId'=>'',                         //客户端分配id
        //'drCbLogPath'=>'/var/log/kafka_dr_cb',  //回调函数保存回调日志的日志文件地址
        //'errCbLogPath'=>'/var/log/kafka_err_cb',//回调函数保存错误日志的日志文件地址
        'defaultLogPath'=>'/var/log/default',   //默认日志路径
        'flushToDisk'=>false,                   //是否将当前消息实时持久化到磁盘
        'ack'=>0                                //是否需要给回调函数返回offset
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
    public static function instance(string $topic, array $appConfig): KafkaProducer
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

        $this->producerConfig = new \RdKafka\Conf();

        // 所有推送的回调地址
        if (isset($this->config['callback'])) {
            $this->producerConfig->setDrmSgCb($this->config['callback']);
        } elseif (isset($this->config['drCbLogPath'])) {
            $this->producerConfig->setDrmSgCb(function ($kafka, $message) {
                //回调日志
                $this->log(json_encode($message, true), $this->config['drCbLogPath']);
            });
        }

        // 所有错误推送的回调日志地址，该日志可追踪错误的推送
        if (isset($this->config['errCbLogPath'])) {
            $this->producerConfig->setErrorCb(function ($kafka, $err, $reason) {
                $content = sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason);
                $this->log($content, $this->config['errCbLogPath']);
            });
        }

        // 设置客户端id,该项可忽略
        if (!empty($this->config['clientId'])) {
            $this->producerConfig->set('client.id', $this->config['clientId']);
        }

        if (!empty($config['brokerList'])) {
            // 生成生产者实例
            $this->producer = new Producer($this->producerConfig);
            // 设置kafka服务器的ip和端口
            $this->producer->addBrokers($this->config['brokerList']);
        } else {
            throw new \Exception('brokerList参数(kafka的ip和端口)没有配置');
        }

        // 是否接收offset位置, （属于消息推送可靠性设置）
        $cf = new \RdKafka\TopicConf();
        if (isset($this->config['ack'])) {
            $cf->set('request.required.acks', $this->config['ack']);
        }
        // 设置超时时间为3秒
        $cf->set("message.timeout.ms", 3000);

        // 生成kafka主题生产实例
        $this->topic = $this->producer->newTopic($topic, $cf);
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

        // 开始推送消息
        $this->log("执行推送：", $this->config['defaultLogPath']);
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
        $this->log($message, $this->config['defaultLogPath']);

        // 非阻塞调用
        $this->producer->poll(0);

        return true;
    }

    /**
     * 刷盘
     */
    public function __destruct()
    {
        if ($this->config['flushToDisk'] == true) {
            //手动持久化当前消息到磁盘
            $this->producer->flush(1000);
        }
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
