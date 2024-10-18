<?php
namespace Tanbolt\Queue;

use Throwable;
use Exception;
use Tanbolt\Queue\Exception\ConnectException;

/**
 * Class QueueResolver
 * @package Tanbolt\Queue
 * Queue 配置中心
 */
class QueueResolver
{
    /**
     * @var array
     */
    private static $drivers = [];

    /**
     * @var array
     */
    private static $unsetDrivers = [];

    /**
     * @var array
     */
    private static $connectionLists = false;

    /**
     * @var string
     */
    private static $connectionDefault = false;

    /**
     * @var FifoAbstract[]
     */
    private static $fifoNodes = [];

    /**
     * 添加一个队列管道驱动
     * @param string $name driver name
     * @param string|FifoAbstract $driver driver class
     */
    public static function addFifoDriver($name, $driver)
    {
        if (!is_string($driver)) {
            $driver = get_class($driver);
        }
        static::$drivers[$name] = $driver;
        unset(static::$unsetDrivers[$name]);
    }

    /**
     * 由名称移除一个队列管道驱动
     * @param $name
     */
    public static function removeFifoDriver($name)
    {
        unset(static::$drivers[$name]);
        static::$unsetDrivers[$name] = true;
    }

    /**
     * 获取指定名称的队列管道驱动
     * @param string $name
     * @return null
     */
    public static function getFifoDriver($name)
    {
        if (isset(static::$drivers[$name])) {
            return static::$drivers[$name];
        }
        if (isset(static::$unsetDrivers[$name])) {
            return null;
        }
        if (class_exists($driver = __NAMESPACE__.'\\Fifo\\'.ucfirst($name))) {
            return static::$drivers[$name] = $driver;
        }
        static::$unsetDrivers[$name] = true;
        return null;
    }

    /**
     * 设置队列服务端连接配置
     * [
     *     名称 => 配置
     *     名称 => 配置
     * ]
     * @param array $lists
     */
    public static function setConnectionLists(array $lists)
    {
        static::$connectionLists = (array) $lists;
    }

    /**
     * 获取已设置服务端连接配置
     * @return array
     */
    public static function getConnectionLists()
    {
        if (static::$connectionLists === false) {
            static::$connectionLists = (array) static::getConfigFromTanbolt('queue_list');
        }
        return static::$connectionLists;
    }

    /**
     * 设置缺省服务端连接名称
     * @param string $default
     */
    public static function setConnectionDefault($default)
    {
        static::$connectionDefault = $default;
    }

    /**
     * 获取缺省服务端连接名称
     * @return null
     */
    public static function getConnectionDefault()
    {
        if (static::$connectionDefault === false) {
            static::$connectionDefault = static::getConfigFromTanbolt('queue');
        }
        return static::$connectionDefault;
    }

    /**
     * try get config from tanbolt config component
     * @param $name
     * @return mixed|null
     */
    private static function getConfigFromTanbolt($name)
    {
        return class_exists('\Tanbolt\Config\Config') ? \Tanbolt\Config\Config::get($name) : null;
    }

    /**
     * 由连接名称获取连接配置
     * @param array|string $connection
     * @return array|null
     */
    public static function getConnection($connection = null)
    {
        if (is_array($connection)) {
            if (!count($connection)) {
                return null;
            }
            $first = reset($connection);
            if (!is_array($first) || !isset($first['driver']) || !isset($first['connection']) || !is_string(key($connection))) {
                $name = $connection;
                ksort($name);
                $connection = [md5(serialize($name)) => $connection];
            }
            return $connection;
        }
        if ($connection === null) {
            $connection = static::getConnectionDefault();
        } else {
            $connection = (string) $connection;
        }
        if (!$connection) {
            return null;
        }
        $lists = static::getConnectionLists();
        return isset($lists[$connection]) ? [$connection => $lists[$connection]] : null;
    }

    /**
     * 由连接名称 或 直接指定连接配置 获取连接驱动对象
     * @param array|string $connection
     * @return FifoAbstract|null
     */
    public static function getQueueDriver($connection = null)
    {
        return static::getQueueDriverInstance($connection);
    }

    /**
     * 类似 getQueueDriver, 不同之处在于获取失败时会抛出异常, 而 getQueueDriver 返回 null
     * @param array|string $connection
     * @return FifoAbstract
     */
    public static function getQueueDriverOrThrow($connection = null)
    {
        return static::getQueueDriverInstance($connection, true);
    }

    /**
     * @param string|array $connection
     * @param bool $throw
     * @return FifoAbstract|null
     * @throws Exception
     * @throws Throwable
     */
    private static function getQueueDriverInstance($connection = null, $throw = false)
    {
        if ($connection = static::getConnection($connection)) {
            list($name, $connection) = [key($connection), current($connection)];
        } else {
            $name = null;
        }
        if (!$connection || !isset($connection['driver']) || !isset($connection['connection'])
            || !is_array($connection['connection'])
        ) {
            $driver = null;
        } else {
            $driver = static::getFifoDriver($connection['driver']);
        }
        if (!$driver) {
            if ($throw) {
                throw new ConnectException('Queue driver not found');
            }
            return null;
        }
        $config = $connection['connection'];
        ksort($config);
        $hash = md5(serialize(compact('name', 'driver', 'config')));
        if (!isset(static::$fifoNodes[$hash])) {
            try {
                /** @var FifoAbstract $driverInstance */
                $driverInstance = new $driver;
                $driverInstance->setConnection($connection['connection'])->setName($name);
                static::$fifoNodes[$hash] = $driverInstance;
            } catch (Exception $e) {
                if ($throw) {
                    throw $e;
                }
                return null;
            } catch (Throwable $e) {
                if ($throw) {
                    throw $e;
                }
                return null;
            }
        }
        return static::$fifoNodes[$hash];
    }
}
