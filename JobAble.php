<?php
namespace Tanbolt\Queue;

use Tanbolt\Queue\Exception\InvalidJobException;
use Tanbolt\Queue\Exception\InvalidQueueException;

/**
 * Class JobAble
 * @package Tanbolt\Queue
 * 队列消息抽象层
 *
 * @property string $connection 消息使用的连接
 * @property string $topic 消息主题
 * @property int $delay 消息延迟时间
 * @property int $timeout 消息超时时长
 * @property int $maxTries 消息最大尝试次数
 *
 * @property string $msgId 消息编号，在一个 topic 中唯一
 * @property string $payload 当前消息序列化过的字符串(实际存储在队列中的字符串)
 * @property int $enqueueTime 消息发送到队列的时间
 * @property int $firstDequeueTime 第一次消费时间
 * @property int $nextVisibleTime 下次可被再次消费的时间
 * @property int $dequeueCount 总共被消费的次数
 * @property bool $deleted 当前消息是否已从队列删除
 * @property bool $released 当前消息是否已重新入栈
 * @property FifoAbstract $fifo 当前消息的管道对象
 */
abstract class JobAble
{
    /**
     * @var array
     */
    private $jobCommonAttributes = [];

    /**
     * @var array
     */
    private static $jobCommonProperties = [
        'connection', 'topic', 'delay', 'timeout', 'maxTries', 'buffer'
    ];

    /**
     * @var array
     */
    private static $jobMessageProperties = [
        'msgId', 'payload',
        'enqueueTime', 'firstDequeueTime', 'nextVisibleTime', 'dequeueCount',
        'deleted', 'released', 'fifo'
    ];

    /**
     * 语法糖函数, 可以通过 JobAble::make(param...)->method() 创建对象并使用链式代码风格, 避免 new class()
     * @return static
     */
    public static function make()
    {
        $args = func_get_args();
        return new static(...$args);
    }

    /**
     * @param $name
     * @param $value
     * @return $this
     */
    private function setCommonAttribute($name, $value)
    {
        if ($value === null) {
            unset($this->jobCommonAttributes[$name]);
        } else {
            $this->jobCommonAttributes[$name] = $value;
        }
        return $this;
    }

    /**
     * @param $name
     * @return string|int|null
     */
    private function getCommonAttribute($name)
    {
        return array_key_exists($name, $this->jobCommonAttributes) ? $this->jobCommonAttributes[$name] : null;
    }

    /**
     * 设置连接信息
     * @param mixed $connection
     * @return $this
     */
    public function setConnection($connection)
    {
        return $this->setCommonAttribute('connection', $connection === null ? null : $connection);
    }

    /**
     * 设置消息主题
     * @param string $topic
     * @return $this
     */
    public function setTopic($topic)
    {
        return $this->setCommonAttribute('topic', $topic === null ? null : (string) $topic);
    }

    /**
     * 设置消息延迟时间
     * @param int $delay
     * @return $this
     */
    public function setDelay($delay)
    {
        return $this->setCommonAttribute('delay', $delay === null ? null : (int) $delay);
    }

    /**
     * 设置延迟到的具体时间
     * @param \DateTime|string $time
     * @return $this|JobAble
     */
    public function delayTo($time)
    {
        if (($date = $time instanceof \DateTime ? clone $time : new \DateTime($time)) &&
            ($time = $date->getTimestamp()) > ($now = time())
        ) {
            return $this->setDelay($time - $now);
        }
        return $this;
    }

    /**
     * 设置超时时长
     * @param int $timeout
     * @return $this
     */
    public function setTimeout($timeout)
    {
        return $this->setCommonAttribute('timeout', $timeout === null ? null : (int) $timeout);
    }

    /**
     * 设置最大尝试次数
     * @param $maxTries
     * @return $this
     */
    public function setMaxTries($maxTries)
    {
        return $this->setCommonAttribute('maxTries', $maxTries === null ? null : (int) $maxTries);
    }

    /**
     * 设置消息实体(置空或重置), 设置后, 当前对象会根据消息实体释放出 $msgId,$payload,$enqueueTime 等属性
     * @param JobMessage|null $message
     * @return $this
     */
    public function setMessage($message)
    {
        if ($message !== null && !$message instanceof JobMessage) {
            throw new \InvalidArgumentException('Message must be NULL or "'.__NAMESPACE__.'\JobMessage" instance.');
        }
        return $this->setCommonAttribute('message', $message);
    }

    /**
     * Queue组件的基本逻辑为:
     * JobAble 对象作为消息任务会被序列化为字符串之后发送到相关队列中
     * 出栈后反序列化为一个 JobAble 对象并调用 consume 方法进行消费
     *
     * 这种逻辑是建立在 生产者和消费者 都是 Queue 组件, 若消费者为其他组件甚至其他语言
     * 入栈一个反序列化的 JobAble 对象并无意义, 因为消费者将无法理解这种数据
     * asBuffer 就是为这种场景而存在的, 如果 JobAble 通过 asBuffer 设置了字符串
     * 那么入栈数据即为所设置的字符串, 出栈数据也为 asBuffer 所设置字符串
     * 这样可以为其他消费者提供有意义的数据
     *
     * @param string $buffer
     * @return $this
     */
    public function asBuffer($buffer)
    {
        return $this->setCommonAttribute('buffer', $buffer);
    }

    /**
     * 初始化当前对象的所有属性, 将置空所有属性
     * @return $this
     */
    public function reset()
    {
        $this->jobCommonAttributes = [];
        return $this;
    }

    /**
     * 将当前 JobAble 对象通过 Queue 添加到队列中
     * @param Queue $queue
     * @return $this
     */
    public function dispatch(Queue $queue)
    {
        $queue->push($this);
        return $this;
    }

    /**
     * 为消费函数 consume 预置的接口: 消费后将消息从队列中移除
     * @return bool
     */
    public function delete()
    {
        /** @var JobMessage $message */
        $message = $this->getCommonAttribute('message');
        if (!$message) {
            throw new InvalidJobException('Job message not configure.');
        }
        return $message->delete();
    }

    /**
     * 为消费函数 consume 预置的接口: 消费后将消息再次入栈, 插入到队列中
     * @param int $delay
     * @return bool
     */
    public function release($delay = 0)
    {
        /** @var JobMessage $message */
        $message = $this->getCommonAttribute('message');
        if (!$message) {
            throw new InvalidJobException('Job message not configure.');
        }
        return $message->release($delay);
    }

    /**
     * @param $name
     * @return bool
     */
    public function __isset($name)
    {
        return in_array($name, self::$jobCommonProperties) ||
            (in_array($name, self::$jobMessageProperties) && ($message = $this->getCommonAttribute('message')));
    }

    /**
     * @param $name
     * @return int|null|string
     */
    public function __get($name)
    {
        if (in_array($name, self::$jobCommonProperties)) {
            return $this->getCommonAttribute($name);
        }
        if (in_array($name, self::$jobMessageProperties) && ($message = $this->getCommonAttribute('message'))) {
            return $message->{$name};
        }
        throw new \InvalidArgumentException('Undefined property: '.__CLASS__.'::$'.$name);
    }

    /**
     * 消费接口的实现
     * @return mixed
     */
    abstract public function consume();
}
