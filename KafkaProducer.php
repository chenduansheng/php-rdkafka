<?php
namespace Common\MQ\Kafka;

use RdKafka\Producer;
use RdKafka\ProducerTopic;

/**
 * @description kafka消息生产者类包, 使用前需安装rdkafka拓展
 * @class KafkaProducer
 */
class KafkaProducer
{
    /**
     * 应用传进来的配置
     * @var array
     */
    private $config = [
        'brokerList'=>'127.0.0.1:9092',         //kafka地址和端口
        'clientId'=>'',                         //客户端分配id
        'drCbLogPath'=>'/var/log/kafka_dr_cb',  //回调函数保存回调日志的日志文件地址
        'errCbLogPath'=>'/var/log/kafka_err_cb',//回调函数保存错误日志的日志文件地址
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
     * @param string $topic
     * @param array $config
     * @return void
     */
    public function __construct(string $topic, array $config)
    {
        $this->config = array_merge($this->config, $config);

        $this->producerConfig = new \RdKafka\Conf();

        if(isset($this->config['drCbLogPath'])) {
            $this->producerConfig->setDrmSgCb(function ($kafka, $message) {
                $this->log(json_encode($message, true), $this->config['drCbLogPath']);
            });
        }
        if(isset($this->config['errCbLogPath'])) {
            $this->producerConfig->setErrorCb(function ($kafka, $err, $reason) {
                $this->log(sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason), $this->config['errCbLogPath']);
            });
        }
        if(!empty($this->config['clientId'])) {
            $this->producerConfig->set('client.id', $this->config['clientId']);
        }
        $this->producer = new Producer($this->producerConfig);
        $this->producer->addBrokers($this->config['brokerList']);

        $cf = new \RdKafka\TopicConf();
        if(isset($this->config['ack'])){
            $cf->set('request.required.acks', $this->config['ack']);
        }
        $this->topic = $this->producer->newTopic($topic, $cf);
    }

    /**
     * @param string $message
     * @return void
     */
    public function send($message)
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, rand(0, 1000));
        $this->producer->poll(0);
    }

    /**
     * 刷盘
     */
    public function __destruct()
    {
        $this->producer->flush(10000);
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