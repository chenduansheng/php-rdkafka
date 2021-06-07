# kafka基础库 - 使用说明



### 1 MQ门面介绍（Facade）

提供快捷调用，无需关乎消息队列

门面文件放在common/Facade/MQ.php



### 2 如何创建消费者：



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
use Stary\Common\MQ\Kafka\KafkaConsumer;
use Stary\Common\Facade\MQ;

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



### 3 如何创建生产者：

```php
<?php

require_once '../vendor/autoload.php';
use Stary\Common\Facade\MQ;



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
 * $result: boolean
**/
$result = MQ::produce('test',['A message for test'], $config);

```



