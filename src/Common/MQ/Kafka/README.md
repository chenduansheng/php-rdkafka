# kafka基础库 - 使用说明



> 基础库做了高度封装，业务层需要写的代码较少，主要是配置，消费和生产都是一行代码

消费：

```
MQ::consum('test', [new UseMessage(), 'main'], $config)->run();
```

生产：

```php
MQ::produce('test',['A message for test'], $config);
```



上面代码的介绍：

| 代码片段                   | 说明                                                     | 类型   |
| -------------------------- | -------------------------------------------------------- | ------ |
| 'test'                     | 队列名                                                   | string |
| [new UseMessage(), 'main'] | 把消费到的消息传给实例new UseMessage()中的main方法去处理 | array  |
| $config                    | 配置，可放到配置文件当中                                 | array  |
|                            |                                                          |        |



### 1 MQ门面介绍（Facade）

提供快捷调用，无需关乎消息队列

门面文件放在基础库的common/MQ/Facade/MQ.php中



### 2 如何开启消费：

开启消费就是通过一行代码：

```php
MQ::consum('test', [new UseMessage(), 'main'], $config)->run();
```



##### 2.1 demo：

```php
<?php

# 获取到的消息交个这个UseMessage类下面的main方法处理。
class UseMessage{
    function main($message){
        // do some thing
        var_dump($message);
        throw new \Exception('test');
        return true;
    }
}


require_once '../vendor/autoload.php';
use StaryPHP\Common\MQ\Kafka\KafkaConsumer;
use StaryPHP\Common\Facade\MQ;

//这个是配置，一般可放入配置文件
$config = [
    'type'=>'kafka',                                                                //消费者分组id
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 192.168.59.100:9095',  //必传、kafka服务器ip和端口
    'groupId'=>'group_1',                                                           //消费者分组id
    'rebalanceLogPath'=>'d:/kafkaRebalanceLog',                                             //kafka重平衡日志
    'defaultLogPath'=>'d:/kafkaAccessLog',                                                  //kafka默认日志路径
    'errorLogPath'=>'d:/kafkaErrorLog',                                                     //kafka错误日志路径
];

/*
//消费消息的方法一：
//这里定义了把消息交给UseMessage对象下面的main方法处理
( KafkaConsumer::instance('test', $config) )
    ->callback([new UseMessage(), 'main'])
    ->run();*/

//消费消息的方法二
//（门面模式（facade））：
/**
 * 参数一：主题名称（即队列名称）
 * 参数二：把消息交给谁处理 (这里把消息交给UseMessage对象下面的main方法处理）
 * 参数三：配置
 **/
MQ::consum('test', [new UseMessage(), 'main'], $config)->run();

```



##### 2.2 广告上报(reporter项目)案例：

reporter/mq/report_to.php

```php
$processor = MQ::consum($queue, [new ZeusReporter($logger), 'report'], $config[$target]);
$processor->run();
```

这里开启了一个广告上报消费者



### 3 如何创建生产者：

创建生产者就是通过一行代码：

```php
MQ::produce('test',['A message for test'], $config);
```



##### 3.1 demo：

```php
<?php

require_once '../vendor/autoload.php';
use StaryPHP\Common\Facade\MQ;



//这个是配置，一般可放入配置文件
$config = [
    'type'=>'kafka',
    'brokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
    'clientId'=>'',
    'drCbLogPath'=>'d:/kafkaProducerDrLog',
    'errCbLogPath'=>'d:/kafkaProducerErrLog',
    'ack'=>1
];

/**
 * kafka特有一致性配置（消息可靠性传输）
 * 如果是在需要一致性的场景下（目前大多数场景不需要），需要知道消息是否发送成功
 * 那么增加异步回调方法，接收发送状况
 **/
$config['callback'] = function ($kafka, $message) {
    var_dump($message);
    if($message->offset<1){
        var_dump('消息推送后未收到offset，未能确认是否推送成功');
    }else{
        var_dump('消息推送成功，保存位置为：'.$message->offset);
    }
};

/*
// 生产消息的方法1
( KafkaProducer::instance('test', $config) )->produce(['A message for test']);*/


// 生产消息的方法2 ~ 门面模式（facade）
/**
 * 参数一：string 主题名称（即队列名称）
 * 参数二：array 要推送的消息内容
 * 参数三：array 配置
 * $result: null
**/
$result = MQ::produce('test',['A message for test'], $config);

```



##### 3.2 广告上报(ad_center项目)案例：

> 异步生产消息

ad_center/src/application/services/CollectService.php

```php
use StaryPHP\Common\MQ\Facade\MQ;
```

```php
try {
    //异步生产消息
    MQ::produce(self::TUBE_NAME, $putData, self::$mqConfig['adReport']);
} catch (Exception $e) {
    Logger::error('Report failed while MQ::produce, message:' . $e->getMessage());
    return false;
}
```

这里创建了一个广告生产者，注意代码MQ::produce(self::TUBE_NAME, $putData, self::$mqConfig['adReport']) 永远只会返回null，它不管你推送成功还是失败，只管推送，并立马返回null，不做同步等待。下面这里介绍如何确保可靠性



### 4 生产者如何进行消息可靠性处理

以广告上报（项目名称：ad_center）为例：

ad_center/src/application/services/CollectService.php

```php
self::$mqConfig['adReport']['callback'] = function ($kafka, $message) {
    //如果推送失败
    if ($message->offset<1) {
        Logger::error($message->payload, 'kafkaProduceFailMessages_');
    } else {
        //如果推送成功
        //稳定后可清除这行代码
        Logger::info(
            "kafkaProduceSuccess:消息推送成功，保存位置为：".$message->offset,
            'kafkaProducerSuccLog'
        );
    }
};
```

这里配置了一个异步回调方法， rdkafka会将成功与否情况回调这个方法



### 5 消费者如何进行消息可靠性处理



##### 5.1 消费失败后，基础库会替你重试三次，以下是基础库重试源码查看：

```php
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
```



5.2 如果重试三次都失败，会自动调用消费者实例（比如广告的消费者实例是reporter/mq/reporter/ZeusReporter.php中的ZeusReporter）中的onfailure方法，可以在onfailure方法中做相关失败处理。

```php
/**
 * 一致性处理，当消费失败，基础库会重试3次，如果继续失败，基础库会调用这个方法，把处理权交给业务端。
 * @param string $topic
 * @param array  $config
 * @return KafkaConsumer
 * @throws \RdKafka\Exception
 */
public function onFailure(string $message)
{
    $this->logger->error("重新入队：adReportDataFail,消息内容：$message");

    $config = C('BEANSTALKD_CONFIG');
    
    //推入到另外一个专门保存失败队列的kafka队列当中。
    MQ::produce('adReportDataFail', $message, $config['adReport']['adReportDataFail']);
}
```



3、这里的失败处理，新建一个存储消费失败的消息的队列， 在onfailure方法中，将失败消息推送到该队列当中，然后对该队列进行消费。由于这次是对失败消息的再次消费，所以为了确保百分百消费成功，需要在队列配置中添加'stopConsumWhileFail'=>true， 添加了这个配置的消费者，当碰到消费失败的消息时，不会提交offset，同时会停掉消费者，等待人工排查处理好后再去开启消费者。



以下是广告上报项目的配置：

reporter/mq/config/server_conf.php.test

```php
//adReport消费失败后，会被传入这个队列，这个队列是要确保百分百消费成功，否则停止消费者并人工排查处理好后再去开启消费者
'adReportFail' => [
    ...
    ...
    'stopConsumWhileFail'=>true//消费失败不提交offset并停止消费者
    ...
    ...
],
```



以下是基础库源码查看：

```php
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
```



### 6 消费分组

##### 6.1 kafka分组概念简单介绍

> 比如现在有a、b两个分组，a分组下面有a1、a2两个消费者，b分组下面有b1、b2两个消费者
>
> 现在来了2条消息，
>
> a分组和b分组都会获得者2条消息， 但是a分组下面的2个消费者是各自获得1条消息，b分组同理。

##### 6.2 我们这里分组，只需修改配置文件当中的groupId

以广告上报（report项目）为例：

report/mq/config/server_conf.php.test

```php
'adReport' => [
    ...
    ...
    'groupId'=>'group_1',  //消费者分组id
    ...
    ...
],
```

### 7 消费重平衡

多见于当同一消费组内的消费者数量发生变化时，就发生重平衡

因为同一消费组内的消费者，在多分区情况下， 他们会被分配不同的分区进行分工协作，

消费重平衡会保存在日志中

以广告上报（report项目）为例：

report/mq/config/server_conf.php.test

```php
'adReport' => [
    ...
    ...
    'rebalanceLogPath'=>$LOG_PATH.'/kafkaConsumRebalanceLog', //kafka重平衡日志
    ...
    ...
],
```

