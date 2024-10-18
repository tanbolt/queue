<?php
namespace Tanbolt\Queue\Fifo;

use Tanbolt\Queue\JobMessage;
use Tanbolt\Queue\FifoAbstract;
use Tanbolt\Database\Database as DB;
use Tanbolt\Queue\Exception\ConnectException;

/**
 * Class Database
 * @package Tanbolt\Queue\Fifo
 *
 * Tanbolt Database 驱动器
 */
class Database extends FifoAbstract
{
    /**
     * @var \Tanbolt\Database\Query\Builder
     */
    private $builder;

    /**
     * @return \Tanbolt\Database\Query\Builder
     */
    private function builder()
    {
        if ($this->builder) {
            return $this->builder->clearWhere();
        }
        $config = $this->getConnection();
        $connection = isset($config['connection']) ? $config['connection'] : null;
        $table = isset($config['table']) ? $config['table'] : 'job_queue';
        return $this->builder = DB::getNode($connection)->table($table);
    }

    /**
     * get left message length
     * @param string $topic
     * @return int
     */
    public function length($topic)
    {
        return $this->builder()->where('topic', $topic)->records();
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
        return $this->insertJobData([compact('message', 'delay', 'topic')]);
    }

    /**
     * push multi messages to queue
     * @param array $messages
     * @return $this
     */
    public function pushMulti(array $messages)
    {
        return $this->insertJobData($messages);
    }

    /**
     * @param array $messages
     * @return $this
     */
    protected function insertJobData(array $messages)
    {
        $now = time();
        $data = [];
        foreach ($messages as $message) {
            $data[] = [
                'topic' => $message['topic'],
                'message' => $message['message'],
                'dequeue_count' => 0,
                'enqueue_time' => $now,
                'first_time' => 0,
                'visible_time' => ($message['delay'] ? $now + (int) $message['delay'] : $now),
            ];
        }
        if (!$this->builder()->insert($data)) {
            throw new ConnectException('Insert data failed');
        }
        return $this;
    }

    /**
     * pop message
     * @param string $topic
     * @return JobMessage|null
     */
    public function pop($topic)
    {
        $now = time();
        $row = $this->builder()
            ->where('topic', $topic)
            ->where('visible_time', '<=', $now)
            ->order('visible_time', true)
            ->getOne(\PDO::FETCH_OBJ);
        if (!$row) {
            return null;
        }
        $update = ['visible_time' => ($visibleTime = $now + 30)];
        $update['dequeue_count'] = ++$row->dequeue_count;
        if (!$row->first_time) {
            $update['first_time'] = $row->first_time = $now;
        }
        if (!$this->builder()->where('id', $row->id)->where('visible_time', $row->visible_time)->update($update)) {
            return $this->pop($topic);
        }
        return new JobMessage(
            $this,
            $topic,
            $row->id,
            $row->message,
            $row->enqueue_time,
            $row->first_time,
            $visibleTime,
            $row->dequeue_count,
            null
        );
    }

    /**
     * delete message
     * @param JobMessage $job
     * @return bool
     */
    public function delete(JobMessage $job)
    {
        return (bool) $this->builder()->where('id', $job->msgId)->delete();
    }

    /**
     * release message
     * @param JobMessage $job
     * @param int $delay
     * @return bool
     */
    public function release(JobMessage $job, $delay = 0)
    {
        return (bool) $this->builder()->where('id', $job->msgId)->update([
            'visible_time' => ($delay ? time() + $delay : time())
        ]);
    }
}
