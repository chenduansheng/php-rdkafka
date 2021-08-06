<?php
namespace StaryPHP\Common\Log;

/**
 * @description access日志容器
 * @class Log
 */
class AccessLogContainer
{
    public static $logId = "";
    public static $time = "";
    public static $project = "";
    public static $main = "";
    public static $error = "";
    public static $commonArg = "";
    public static $statis = "";

    public function setlogId($logId)
    {
        self::$logId = $logId;
    }
    public function setProject($project)
    {
        self::$project = $project;
    }
    public function setMain($main)
    {
        self::$main = $main;
    }
    public function setError($error)
    {
        self::$error = $error;
    }
    public function setCommonArg($commonArg)
    {
        self::$commonArg = $commonArg;
    }
    public function setStatis($statis)
    {
        foreach ($statis as $key=>$val){
            self::$statis[$key] = $val;
        }
    }

    public function format(){
        $msgArray = [];

        /*基本信息*/
        $msgArray['type'] = "ACCESS";
        $msgArray['time'] = str_replace(' ', 'T', self::$time);
        $msgArray['logId'] = get_log_id();
        $msgArray['project'] = LogConfig::$options['LOG_NAME'];
        $msgArray['uri'] = substr($this->uri,0,stripos($this->uri ,"?"));

        $msgArray['main'] = self::$main;

        $msgArray['statis'] = self::$statis;

        $msgArray['commonInfo'] = self::$commonArg;

        $msgJson = json_encode($msgArray).PHP_EOL;

        return $msgJson;
    }
}
