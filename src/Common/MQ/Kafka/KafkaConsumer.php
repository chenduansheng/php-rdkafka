<?php

namespace Stary\Common\MQ\Kafka;

use RdKafka\KafkaConsumer as RkConsumer;
use Stary\Common\MQ\Kafka\KafkaProducer;
use RdKafka\Message;
use RdKafka\Conf;
use DingTalkApi;

/**
 * @description kafka消息消费者类包, 使用前需安装rdkafka拓展
 * @class KafkaConsumer
 */
class KafkaConsumer
{
    /**
     * 失败超过10条就告警
     * @const intager
     */
    const ERROR_THRESHOLD_TO_ALARM = 10;

    /**
     * 定义失败重试次数 (如果消费者获取到消息后，处理消息失败，就再试几次)
     * @const intager
     */
    const RETRY_TIMES_AFTER_FAIL = 3;

    /**
     * 定义错误编号
     * @const intager
     */
    const ERROR_NO_CALL_BACK_FAIL = "0001";

    /**
     * 默认配置项，应用传值过来了就会用传过来的，其中brokerList为必传项
     * @var string
     */
    private $config = [
        'brokerList'=>'',                                   //连接kafka的broker - 【只有此项是必传】
        'groupId'=>'default',                               //设置分组id
        'clientId'=>'',                                     //设置客户端id
        'rebalanceLogPath'=>'/var/log/KafkaRebalanceLog',   //记录消费者重平衡的日志路径
        'defaultLogPath'=>'/var/log/default',               //默认日志路径
        'errorLogPath'=>'var/log/error',                    //默认错误日志路径
        'stopConsumWhileFail'=>false,                       //出现消费失败情况是否立马停止消费
        'maxConsumNum'=>0,                                  //最大消费条数（设置为0就是不限制）
        'expireTime'=>0,                                    //消费者生存时间（设置为0就是不限制）
        'autoOffsetReset'=>'smallest'                       //消费者初始偏移位置
    ];

    /**
     * KafkaConsumer单例
     * @var KafkaConsumer
     */
    private $callback = [];

    /**
     * KafkaConsumer单例
     * @var KafkaConsumer
     */
    private static $instance = [];

    /**
     * Rdkafka消费实例
     * @var RkConsumer
     */
    public $consumer;

    /**
     * topic主题名
     * @var string
     */
    private $topic;

    /**
     * 开关
     * @var boolean
     */
    public $isStoped;

    /**
     * 获得KafkaConsumer单例下的RdConsumer
     * @param string $topic
     * @param array  $config
     * @return KafkaConsumer
     * @throws \RdKafka\Exception
     */
    public static function instance(string $topic, array $config): KafkaConsumer
    {
        if (!isset(self::$instance[$topic])) {
            self::$instance[$topic] = new self($topic, $config);
        }

        return self::$instance[$topic];
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

        // 设置重平衡回调地址
        $conf->setRebalanceCb([$this, 'kafkaRebalanceCb']);

        // 是指分组id
        $conf->set('group.id', $this->config['groupId']);

        // 设置clientid,这个可以忽略
        if (!empty($this->config['clientId'])) {
            $conf->set('client.id', $this->config['clientId']);
        }

        // 设置kafka服务器ip和端口
        $conf->set('metadata.broker.list', $this->config['brokerList']);

        //设置使用手动提交offset
        $conf->set('enable.auto.commit', 'false');

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        // 初始偏移位置
        if (!empty($this->config['autoOffsetReset'])) {
            $conf->set('auto.offset.reset', $this->config['autoOffsetReset']);
        }

        $consumer = new \RdKafka\KafkaConsumer($conf);

        // Subscribe to topic
        $consumer->subscribe([$this->topic]);

        $this->consumer = $consumer;
    }

    /**
     * 设置处理消息的回调类和函数
     * @param object $callback
     * @return $this
     */
    public function callback($callback)
    {
        $this->callback = $callback;
        return $this;
    }

    /**
     * 执行实际消费者
     * @param object $callback 处理消息的类和方法
     * @param object  $message 消息
     * @return void
     * @throws \Exception
     */
    private function doRun($callback, $message)
    {
        $isSuccess = false;

        $content = $message->payload;
        $executeInfo = "partition=$message->partition offset=$message->offset ".
            json_encode($callback). "message=$content";

        $startTime = microtime(true) * 1000;

        try {
            $isSuccess = $this->doCallBack($callback, $message);
        } catch (Exception $e) {
            $this->log(
                "[执行callback时失败：分区id-$message->partition-偏移量-$message->offset]".PHP_EOL,
                $this->config['errorLogPath']
            );
        }

        // 消息使用完毕后，需要return ture，否则这里会人为消费失败
        if ($isSuccess !== true) {
            //如果设置了碰到错误不提交offset并立马停止消费者
            if ($this->config['stopConsumWhileFail'] == true) {
                $this->stop();
                $this->consumer->close();
                DingTalkApi::sendRobotMessage(
                    "【必须处理】消息队列-kafka-消费失败-已关闭消费者：". PHP_EOL .
                    "kafka队列名称: ". $this->topic . PHP_EOL .
                    'server name: ' . gethostname() . PHP_EOL .
                    'tips: please check the error logs ' . PHP_EOL .
                    'path: ' . $this->config['errorLogPath']
                );
                return false;
            }
            $this->log(self::ERROR_NO_CALL_BACK_FAIL."消费失败 $executeInfo", $this->config['errorLogPath']);
            $result = $this->onFailure($callback, $content);
            if ($result === false) {
                $this->log(self::ERROR_NO_CALL_BACK_FAIL."没处理失败 $executeInfo", $this->config['errorLogPath']);
            }
        }

        $costTime = round(microtime(true) * 1000 - $startTime, 3);
        $executeInfo .= " cost=$costTime";

        $this->log($executeInfo, $this->config['defaultLogPath']);
        try {
            $this->consumer->commitAsync($message); // 异步手动提交offset(不会阻塞poll)
            $this->log("[正常提交offset：分区id-$message->partition-偏移量-$message->offset]".PHP_EOL);
        } catch (\Exception $e) {
            $isSuccess = false;
            $content = "[发送提交请求失败：分区id-$message->partition-偏移量-$message->offset]".PHP_EOL;
            $this->log($content, $this->config['errorLogPath']);
        }

        return $isSuccess;
    }

    /**
     * 消费者获取到消息后，处理消息失败后执行了重试，重试依然不成功就执行下面的逻辑
     * @param object $content
     * @param object $message
     * @return boolean
     * @throws \Rdkafka\Exception
     */
    private function onFailure($callback, string $content)
    {
        $object = $callback['0'];
        $isSuccess = false;

        //获取到的消息，前面处理3次后仍然失败，就到这里调用消费者的onFailure方法。
        if (method_exists($object, 'onFailure')) {
            $isSuccess = call_user_func_array([$object, 'onFailure'], [$content]);
        }

        return $isSuccess;
    }

    /**
     * 消费者获取到消息后，处理消息失败后执行重试
     * @param object $callback
     * @param object $message
     * @return boolean
     * @throws \Rdkafka\Exception
     */
    private function doCallBack($callback, $message)
    {

        //默认执行失败
        $isSuccess = false;
        //记录失败重试次数
        $reTryTimes = 0;

        //重试(RETRY_TIMES_AFTER_FAIL)次， 直到成功
        while ($isSuccess == false && $reTryTimes < self::RETRY_TIMES_AFTER_FAIL) {
            try {
                $isSuccess = call_user_func_array($callback, [json_decode($message->payload, true)]);
            } catch (\Exception $e) {
                $this->log("第".$reTryTimes."次失败".$e->getmessage(), $this->config['errorLogPath']);
                $isSuccess = false;
            }
            $reTryTimes++;
        }

        return $isSuccess;
    }

    /**
     * 执行实际消费者
     * @param object $callback
     * @return void
     * @throws \Rdkafka\Exception
     */
    public function run(): void
    {
        $this->log('[开始消费] Begin consume');

        $consumNum = 0;
        $errNum = 0;
        $startTime = time();

        do {
            $message = $this->consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    try {
                        //把消息内容$message 交给$this->callback实例方法去处理，这也是消费者的最终目的。
                        $isSuccess = $this->doRun($this->callback, $message);
                    } catch (Exception $e) {
                        $isSuccess = false;
                        $this->log("消费异常 exception={$e->getMessage()}", $this->config['errorLogPath']);
                        //$this->consumer->close();
                        //return;
                    }

                    if (!$isSuccess) {
                        $errNum++;
                    }
                    // 错误超过10条则发送消息给钉钉
                    if ($errNum >= self::ERROR_THRESHOLD_TO_ALARM) {
                        DingTalkApi::sendRobotMessage(
                            "消息队列-kafka-消费-告警：". PHP_EOL .
                            "kafka队列名称: ". $this->topic . PHP_EOL .
                            "msg: report failure $errNum >= " . self::ERROR_THRESHOLD_TO_ALARM . PHP_EOL .
                            'server name: ' . gethostname() . PHP_EOL .
                            'tips: please check the error logs ' . PHP_EOL .
                            'path: ' . $this->config['errorLogPath']
                        );
                    }
                    $errNum = 0;

                    $consumNum++;
                    if (!empty($this->config['maxConsumNum'])) {
                        if ($consumNum > $this->config['maxConsumNum']) {
                            $this->log("消费条数达到maxConsumNum限制,退出消费");
                            // 关闭开关
                            $this->stop();
                        }
                    }
                    if (!empty($this->config['expireTime'])) {
                        if ((time() - $startTime) > $this->config['expireTime']) {
                            $this->log("消费时间达到expireTime限制,退出消费");
                            // 关闭开关
                            $this->stop();
                        }
                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    //当消费者长时间没收到消息时，会进入这里，但进入这里不意味着有问题
                    echo "Timed out! But its not an error, Its means wait a long time\n";
                    break;
                default:
                    //一般是kafka长时间死掉了没复活，需要人为排查和干预
                    $this->log("消费异常 rd拓展exception={$message->errstr()}", $this->config['errorLogPath']);
                    DingTalkApi::sendRobotMessage(
                        "kafka消费者异常，rd拓展错误号: ".$message->errstr(). PHP_EOL .
                        "kafka: ". $this->topic . PHP_EOL .
                        "msg: report failure 【Maybe kafka is die！】" . PHP_EOL .
                        'server name: ' . gethostname() . PHP_EOL .
                        'tips: please check the error logs ' . PHP_EOL .
                        'path: ' . $this->config['errorLogPath']
                    );
                    break;
            }
        } while (!$this->isStopped());
    }

    public function execute()
    {
        return $this->run();
    }

    /**
     * kafka重平衡回调
     * 发生了重平衡，需保存重平衡日志，方便追踪，也方便查看重平衡之后的消费分区状况
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
                throw new \Exception($err);
        }
    }


    /**
     * @param string $message 日志内容
     * @param string $path    日志路径
     * @return void
     */
    private function log($message, $path = '')
    {
        if (!$path) {
            $path = $this->config['defaultLogPath'];
        }
        $dateTime = date('Y-m-d H:i:s');

        $date = date('Ymd', time());
        $path = $path.'.'.$this->topic.'.consumer.kafka.log'.$date;

        error_log("[$dateTime] $message".PHP_EOL.PHP_EOL, 3, $path);
    }

    public function stop()
    {
        $this->isStoped = true;
    }

    public function isStopped()
    {
        return $this->isStoped;
    }
}
