<?php
namespace Tanbolt\Queue\Fifo;

use Tanbolt\Fqueue\Fqueue;
use Tanbolt\Queue\JobMessage;
use Tanbolt\Queue\FifoAbstract;
use Tanbolt\Queue\Exception\ConnectException;

/**
 * Class File
 * @package Tanbolt\Queue\Fifo
 *
 * Tanbolt Fqueue 驱动器
 */
class File extends FifoAbstract
{
    /**
     * @var Fqueue
     */
    private $fileQueue = null;

    /**
     * @return Fqueue
     */
    private function driver()
    {
        if ($this->fileQueue) {
            return $this->fileQueue;
        }
        $connection = $this->getConnection();
        $path = isset($connection['path']) ? $connection['path'] : null;
        if (!$path) {
            throw new ConnectException('File queue path not configure');
        }
        return $this->fileQueue = new Fqueue($path);
    }

    /**
     * @param $message
     * @param array $meta
     * @return string
     */
    private function getPayload($message, $meta = null)
    {
        if (is_array($meta)) {
            $meta['payload'] = $message;
            return serialize($meta);
        }
        return $message;
    }

    /**
     * get left message length
     * @param string $topic
     * @return int
     */
    public function length($topic)
    {
        return $this->driver()->length($topic);
    }

    /**
     * push a message to queue
     * @param string $message
     * @param int $delay
     * @param string $topic
     * @return $this
     */
    public function push($message, $delay = 0, $topic)
    {
        $this->driver()->push($this->getPayload($message), $delay, $topic);
        return $this;
    }

    /**
     * push multi messages to queue
     * @param array $messages
     * @return $this
     */
    public function pushMulti(array $messages)
    {
        if (!$messages) {
            return $this;
        }
        $driver = $this->driver();
        foreach ($messages as $message) {
            $driver->setMessage($this->getPayload($message['message']), $message['delay'], $message['topic']);
        }
        $driver->send();
        return $this;
    }

    /**
     * pop message
     * @param string $topic
     * @return JobMessage|null
     */
    public function pop($topic)
    {
        $data = $this->driver()->pop($topic, true);
        if (!$data) {
            return null;
        }
        $msgId = null;
        $firstDequeueTime = null;
        if (is_array($message = @unserialize($data['message'])) && isset($message['payload'])) {
            $dequeueCount = 1;
            $payload = $message['payload'];
            if (isset($message['ct'])) {
                $dequeueCount = $message['ct'] + 1;
                $msgId = isset($message['id']) ? $message['id'] : null;
            }
            if (isset($message['ft'])) {
                $firstDequeueTime = $message['ft'];
            }
        } else {
            $dequeueCount = 1;
            $payload = $data['message'];
        }
        if (!$msgId) {
            $msgId = $data['label'] . '-' . $data['offset'];
        }
        if (!$firstDequeueTime) {
            $firstDequeueTime = time();
        }
        return new JobMessage(
            $this,
            $topic,
            $msgId,
            $payload,
            $data['time'],
            $firstDequeueTime,
            null,
            $dequeueCount,
            $msgId
        );
    }

    /**
     * delete message
     * @param JobMessage $job
     * @return bool
     */
    public function delete(JobMessage $job)
    {
        return true;
    }

    /**
     * release message
     * @param JobMessage $job
     * @param int $delay
     * @return bool
     */
    public function release(JobMessage $job, $delay = 0)
    {
        $message = $this->getPayload($job->payload, [
            'id' => $job->msgId,
            'ct' => $job->dequeueCount,
            'ft' => $job->firstDequeueTime
        ]);
        $this->driver()->push($message, $delay, $job->topic);
        return true;
    }
}
