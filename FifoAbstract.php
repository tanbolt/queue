<?php
namespace Tanbolt\Queue;

/**
 * Class FifoAbstract
 * @package Tanbolt\Queue
 * 出入栈管道驱动
 */
abstract class FifoAbstract
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var array
     */
    protected $connection;

    /**
     * 设置管道名称
     * @param string $name
     * @return $this
     */
    public function setName($name)
    {
        $this->name = $name;
        return $this;
    }

    /**
     * 获取管道名称
     * @return string
     */
    public function getName()
    {
        if (!$this->name && $this->connection) {
            $connection = $this->connection;
            ksort($connection);
            $this->name = md5(serialize($connection));
        }
        return $this->name;
    }

    /**
     * 设置管道连接配置
     * @param array $connection
     * @return $this
     */
    public function setConnection($connection)
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * 获取管道连接配置
     * @return array
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * 获取当前管道下指定主题的未消费消息总数
     * @param string $topic
     * @return int
     */
    abstract public function length($topic);

    /**
     * 入栈一条消息到当前管道
     * @param string $message 消息内容
     * @param int $delay 延迟时间
     * @param string $topic 主题
     * @return $this
     */
    abstract public function push($message, $delay = 0, $topic);

    /**
     * 批量入栈多条消息到当前管道
     * @param array $messages
     * @return $this
     */
    abstract public function pushMulti(array $messages);

    /**
     * 从指定主题出栈一条消息
     * @param string $topic
     * @return JobMessage|null
     */
    abstract public function pop($topic);

    /**
     * 删除一条消息
     * @param JobMessage $job
     * @return bool
     */
    abstract public function delete(JobMessage $job);

    /**
     * 重新入栈一条消息
     * @param JobMessage $job
     * @param int $delay
     * @return bool
     */
    abstract public function release(JobMessage $job, $delay = 0);
}
