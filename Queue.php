<?php
namespace Tanbolt\Queue;

/**
 * Class Queue
 * @package Tanbolt\Queue
 * 队列消息生产者
 */
class Queue
{
    /**
     * @var array|string
     */
    private $connection;

    /**
     * @var array
     */
    private $batch = [];

    /**
     * 创建 Queue 对象
     * @param mixed $connection 驱动配置信息
     */
    public function __construct($connection = null)
    {
        $this->connection = $connection;
    }

    /**
     * 获取 queue 驱动对象
     * @param mixed $connection 驱动配置信息,若不指定,则使用当前对象的默认配置
     * @return FifoAbstract
     */
    public function fifo($connection = null)
    {
        $connection = $connection ?: $this->connection;
        return QueueResolver::getQueueDriverOrThrow($connection);
    }

    /**
     * 获取指定主题的队列消息数
     * @param string $topic 主题名称
     * @param mixed $connection 驱动配置信息,若不指定,则使用当前对象的默认配置
     * @return int
     */
    public function length($topic = null, $connection = null)
    {
        return $this->fifo($connection)->length($this->getTopic($topic, $connection));
    }

    /**
     * 添加一个任务到队列中
     * @param JobAble $job
     * @return $this
     */
    public function push(JobAble $job)
    {
        $row = $this->parseJob($job);
        $this->fifo($row['connection'])->push($row['message'], $row['delay'], $row['topic']);
        return $this;
    }

    /**
     * 准备一个待添加的任务
     * @param JobAble $job
     * @return $this
     */
    public function setBatch(JobAble $job)
    {
        $this->batch[] = $job;
        return $this;
    }

    /**
     * 将所有使用 setBatch 添加的任务批量添加到队列中
     * @return $this
     */
    public function pushBatch()
    {
        $batch = [];
        foreach ($this->batch as $job) {
            $job = $this->parseJob($job, true);
            if (!($name = $job['name'])) {
                continue;
            }
            if (!isset($batch[$name])) {
                $batch[$name] = [
                  'connection' => $job['connection'],
                  'jobs' => [],
                ];
            }
            unset($job['name'], $job['connection']);
            $batch[$name]['jobs'][] = $job;
        }
        foreach ($batch as $message) {
            $this->fifo($message['connection'])->pushMulti($message['jobs']);
        }
        $this->batch = [];
        return $this;
    }

    /**
     * @param JobAble $job
     * @param bool $withName
     * @return array
     */
    protected function parseJob(JobAble $job, $withName = false)
    {
        $row = [
            'connection' => $job->connection,
            'delay' => $job->delay ?: 0,
            'topic' => $this->getTopic($job->topic, $job->connection),
        ];
        if ($job->buffer) {
            $row['message'] = $job->buffer;
        } else {
            $message = clone $job;
            $row['message'] = serialize($message->setMessage(null)->setTopic(null)->setConnection(null));
        }
        if ($withName) {
            $row['name'] = ($name = QueueResolver::getConnection($job->connection)) ? key($name) : null;
        }
        return $row;
    }

    /**
     * @param null $topic
     * @param null $connection
     * @return string
     */
    private function getTopic($topic = null, $connection = null)
    {
        if ($topic) {
            return (string) $topic;
        }
        $conn = ($conn = QueueResolver::getConnection($connection)) ? current($conn) : null;
        return $conn && isset($conn['topic']) && is_string($conn['topic']) ? $conn['topic'] : 'default';
    }

    /**
     * for tanbolt container
     * @return bool
     */
    public static function __shared()
    {
        return true;
    }

    /**
     * 情况已填充的批量消息
     * @return $this
     */
    public function __destruct()
    {
        $this->batch = [];
        return $this;
    }
}
