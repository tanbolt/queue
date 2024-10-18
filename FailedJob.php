<?php
namespace Tanbolt\Queue;

class FailedJob
{
    /**
     * @var JobAble
     */
    private $job;

    /**
     * @var \Exception|\Throwable
     */
    private $exception;

    /**
     * @var string
     */
    private $workerName;


    public function __construct(JobAble $job, $exception, $workerName)
    {
        $this->job = $job;
        $this->exception = $exception;
        $this->workerName = $workerName;
    }

    /**
     * @return JobAble
     */
    public function job()
    {
        return $this->job;
    }

    /**
     * @return \Exception|\Throwable
     */
    public function exception()
    {
        return $this->exception;
    }

    /**
     * @return string
     */
    public function workerName()
    {
        return $this->workerName ?: $this->job->fifo->getName();
    }

    /**
     * @return string
     */
    public function payload()
    {
        $payload = ['command' => get_class($this->job)];
        $properties = ['connection', 'topic', 'msgId', 'payload', 'enqueueTime', 'firstDequeueTime', 'dequeueCount'];
        foreach ($properties as $property) {
            $payload[$property] = $this->job->{$property};
        }
        return serialize($payload);
    }

    /**
     * @param $payload
     */
    public static function runPayload($payload)
    {
        $payload = @unserialize($payload);
        $job = is_array($payload) && isset($payload['payload']) ? @unserialize($payload['payload']) : null;
        if (!$job instanceof JobAble) {
            throw new \RuntimeException('Can not resolve jobAble instance');
        }
        $jobMessage = new JobMessage(
            QueueResolver::getQueueDriverOrThrow($payload['connection']),
            $payload['topic'],
            $payload['msgId'],
            $payload['payload'],
            $payload['enqueueTime'],
            $payload['firstDequeueTime'],
            null,
            $payload['dequeueCount']
        );
        $jobMessage->deleted = true;
        $job->setMessage($jobMessage)->setTopic($payload['topic'])->setConnection($payload['connection']);
        $job->consume();
    }
}
