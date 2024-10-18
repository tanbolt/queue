<?php
namespace Tanbolt\Queue;

class JobMessage
{
    /**
     * @var FifoAbstract
     */
    public $fifo;

    /**
     * topic name
     * @var string
     */
    public $topic = null;

    /**
     * 消息编号，在一个 topic 中唯一
     * @var string
     */
    public $msgId = null;

    /**
     * @var string
     */
    public $payload = null;

    /**
     * 消息发送到队列的时间
     * @var int
     */
    public $enqueueTime = null;

    /**
     * 第一次消费时间
     * @var int
     */
    public $firstDequeueTime = null;

    /**
     * 下次可被再次消费的时间
     * @var int
     */
    public $nextVisibleTime = null;

    /**
     * 总共被消费的次数
     * @var int
     */
    public $dequeueCount = null;

    /**
     * 当前消息是否已从队列删除
     * @var bool
     */
    public $deleted = false;

    /**
     * 当前消息是否已重新入栈
     * @var bool
     */
    public $released = false;

    /**
     * 消息产生的临时句柄，用于删除和修改处于 Inactive 消息，NextVisibleTime 之前有效
     * @var string
     */
    private $receiptHandle = null;

    /**
     * JobMessage constructor.
     * @param FifoAbstract $fifo
     * @param null $topic
     * @param null $msgId
     * @param null $payload
     * @param null $enqueueTime
     * @param null $firstDequeueTime
     * @param null $nextVisibleTime
     * @param null $dequeueCount
     * @param null $receiptHandle
     */
    public function __construct(
        FifoAbstract $fifo = null,
        $topic = null,
        $msgId = null,
        $payload = null,
        $enqueueTime = null,
        $firstDequeueTime = null,
        $nextVisibleTime = null,
        $dequeueCount = null,
        $receiptHandle = null
    ) {
        $this->fifo = $fifo;
        $this->msgId = $msgId;
        $this->topic = $topic;
        $this->payload = $payload;
        $this->enqueueTime = $enqueueTime;
        $this->firstDequeueTime = $firstDequeueTime;
        $this->nextVisibleTime = $nextVisibleTime;
        $this->dequeueCount = $dequeueCount;
        $this->receiptHandle = $receiptHandle;
    }

    /**
     * @return null|string
     */
    public function receiptHandle()
    {
        return $this->receiptHandle;
    }

    /**
     * @return bool
     */
    public function delete()
    {
        if (!$this->deleted && !$this->released) {
            return $this->fifo->delete($this) ? $this->deleted = true : false;
        }
        return true;
    }

    /**
     * @param int $delay
     * @return bool
     */
    public function release($delay = 0)
    {
        if (!$this->released && !$this->deleted) {
            return $this->fifo->release($this, $delay) ? $this->released = true : false;
        }
        return true;
    }
}
