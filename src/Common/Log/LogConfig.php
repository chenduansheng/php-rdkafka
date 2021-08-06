<?php
class LogConfig
{
    public static $options = [
        'PROJECT_NAME' => '',
        'LOG_PATH' => '',
        'LOG_NAME' => '',
        'LOG_POSTFIX' => '',
    ];

    public static function setOption($options){
        self::$options = $options;
    }
}