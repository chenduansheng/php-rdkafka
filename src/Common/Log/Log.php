<?php

class Log
{
    const LOG_TYPE_FATAL   = 'FATAL';
    const LOG_TYPE_ERROR   = 'ERROR';
    const LOG_TYPE_WARNING = 'WARNING';
    const LOG_TYPE_INFO    = 'INFO';
    const LOG_TYPE_DEBUG   = 'DEBUG';
    const LOG_TYPE_ACCESS  = 'ACCESS';


    private $logId;
    private $clientIp;
    private $requestUri;

    public static $instance = null;

    public function __construct()
    {
    }

    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new Log();
        }
        return self::$instance;
    }

    public static function debug($msg)
    {
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_DEBUG, $msg);
        return $ret;
    }

    public static function info($msg)
    {
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_INFO, $msg);
        return $ret;
    }

    public static function access($msg)
    {
        $msg = \StaryPHP\Common\Log\AccessLogContainer::format();
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_ACCESS, $msg);
        return $ret;
    }

    public static function warning($msg)
    {
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_WARNING, $msg);
        return $ret;
    }

    public static function error($msg)
    {
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_ERROR, $msg);
        return $ret;
    }

    public static function fatal($msg)
    {
        $ret = self::getInstance()->writeLog(self::LOG_TYPE_FATAL, $msg);
        return $ret;
    }

    protected function writeLog($logType, $msg)
    {
        $logPath  = $this->getLogPath($logType);
        $logTime  = $this->getLogTime();
        $log_id    = $this->getLogId();
        $clientIp = $this->getClientIp();
        $uri   = $this->getUri();
        $trace = debug_backtrace();
        $fileName = $trace['1']['file'];
        $lineNo   = $trace['1']['line'];

        $content = [
            'time'=>$logTime,
            'project'=>LogConfig::$options['PROJECT_NAME'],
            'logid'=>$log_id,
            'type'=>$logType,
            'filename'=>$fileName,
            'lineno'=>$lineNo,
            'uri'=>$uri,
            'msg'=>$msg,
            'clientIp'=>$clientIp,
        ];

        return file_put_contents($logPath, $content, FILE_APPEND);
    }

    protected function getLogPath($logType)
    {
        $logPath = LogConfig::$options['LOG_PATH'];
        if (!is_dir($logPath)) {
            @mkdir($logPath, 0777, true);
        }
        switch ($logType){
            case 'LOG_TYPE_FATAL':
            case 'LOG_TYPE_ERROR':
            case 'LOG_TYPE_WARNING':
                $logPath = $logPath . LogConfig::$options['LOG_NAME'] .'error';
            case 'LOG_TYPE_ACCESS':
                $logPath = $logPath . LogConfig::$options['LOG_NAME'] .'access';
            default:
                $logPath = $logPath . LogConfig::$options['LOG_NAME'];
        }
        $logPath = $logPath . LogConfig::$options['LOG_POSTFIX'];

        return $logPath;
    }
    
    public function getLogId()
    {
        if (!$this->logId) {
            $this->logId = get_log_id();
        }
        return $this->logId;
    }

    protected function getLogTime()
    {
        return $logTime = str_replace(date('Y-m-d H:i:s'),' ', 'T');
    }

    protected function getClientIp()
    {
        /*{{{*/
        if (!$this->clientIp) {
            if (isset($_SERVER["HTTP_X_FORWARDED_FOR"])) {
                $ip = $_SERVER["HTTP_X_FORWARDED_FOR"];
            } elseif (isset($_SERVER["HTTP_clientIp"])) {
                $ip = $_SERVER["HTTP_clientIp"];
            } elseif (isset($_SERVER["REMOTE_ADDR"])) {
                $ip = $_SERVER["REMOTE_ADDR"];
            } elseif (getenv("HTTP_X_FORWARDED_FOR")) {
                $ip = getenv("HTTP_X_FORWARDED_FOR");
            } elseif (getenv("HTTP_clientIp")) {
                $ip = getenv("HTTP_clientIp");
            } elseif (getenv("REMOTE_ADDR")) {
                $ip = getenv("REMOTE_ADDR");
            } else {
                $ip = "Unknown";
            }
            $this->clientIp = $ip;
        }
        return $this->clientIp;
    }

    protected function getUri()
    {
        if (!$this->requestUri && isset($_SERVER['REQUEST_URI'])) {
            $this->requestUri = $_SERVER['REQUEST_URI'];
        }
        return $this->requestUri;
    }
}
