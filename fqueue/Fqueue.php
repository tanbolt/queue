<?php
namespace Tanbolt\Fqueue;

use Throwable;
use Tanbolt\Fqueue\Exception\IoException;
use Tanbolt\Fqueue\Exception\FileErrorException;
use Tanbolt\Fqueue\Exception\CreateFailedException;

/**
 * Class Fqueue
 * @package Tanbolt\Fqueue
 * 文件系统列队
 *
 * 会在设置的 folder 下创建不同的 topic 文件夹用于保存每个 topic 下的队列消息
 * 每个 topic 下可能产生以下文件：
 *
 *    - partitionIndex
 *    分片数据索引文件, 按升序记录当前所有 dat|index 分片数据组的起始序号（即文件名）
 *
 *    - 0000000000.dat
 *    - 0000000000.index
 *    - .........
 *    多组分片(Partition)数据 （dat用于存储消息数据，index 索引了 dat 中所有消息序号对应的位置offset信息）
 *    每个 dat 的最大容量，超过容量后, 会创建一组新的 dat|index 分片文件，文件名为该组 分配数据 存储消息的起始序号
 *    可使用 setPartitionSize() 设置该最大容量为多少 MB
 *
 *    - label_[int]
 *    文件夹，在索引 partitionIndex 和 分片文件 dat|index 达到上限，会将数据备份到 label_[int] 中
 *
 *    - delayMessage
 *    延迟消息数据存储，会在读取（消息出栈）时，根据延迟时间，将延迟时间到了的消息写入到 dat|index 分片数据中
 *
 *    - label
 *    从分片数据的命名可以看出,一个文件夹下可存储消息数量有上限（默认 labelSize：2147483647）
 *    当消息总数达到上限, 当达到消息上限，会再次创建创建一组文件，label 记录当前一共创建了几个 label
 *
 *    - current
 *    L
 *
 * 在运行工程中，可能会产生以下临时文件
 *
 *    - lock
 *    若当前 label 已写满（消息数量不少于 labelSize），会创建一个 lock 空文件来作为标记
 *
 *
 * 入栈消息流程
 *    根据 partitionIndex 查找当前存储分片
 *    ->  判断分片大小是否超出最大值 -> 超出, 创建新的分片, 并记录到 partitionIndex
 *    -> 存储数据到分片中
 *
 * 入栈延迟消息流程
 *    确认是否刚好在重建延迟消息中, 等待至重建结束
 *    > 保存消息到 delayMessage 文件
 *
 *
 */
class Fqueue
{
    const DEFAULT_TOPIC = 'default';

    const HANDLER_DAT = 'dat';
    const HANDLER_INDEX = 'index';
    const HANDLER_CURRENT = 'current';
    const HANDLER_DELAY_MESSAGE = 'delayMessage';
    const HANDLER_PARTITION_INDEX = 'partitionIndex';

    /**
     * 所有主题消息存储的根目录
     * @var string
     */
    private $folder;

    /**
     * 单个分片文件的最大尺寸（单位：MB）
     * @var int
     */
    private $partitionSize = 500;

    /**
     * 每个 label 存储消息的条数
     * @var int
     */
    private $labelSize = 0x7FFFFFFF;

    /**
     * @var array
     */
    private $messageStore = [];

    /**
     * @var array
     */
    private $dirCheckCache = [];

    /**
     * @var array
     */
    private $resourceHandler = [];

    /**
     * @var array
     */
    private $partitionLabel = [];

    /**
     * @var int
     */
    private $errorLever;

    /**
     * 创建 Fqueue 对象
     * @param string $folder 保存文件夹路径
     * @param ?int $partitionSize 单个分片允许的最大文件尺寸（单位：MB）
     */
    public function __construct(string $folder, int $partitionSize = null)
    {
        $this->setFolder($folder);
        if ($partitionSize) {
            $this->setPartitionSize($partitionSize);
        }
    }

    /**
     * 销毁 Fqueue 对象 并关闭所有已打开文件指针
     */
    public function __destruct()
    {
        $this->closeWriteHandler()->closeReadHandler();
    }

    /**
     * 设置每个 label 可存储的最大消息条数。
     * > **注意**：这个主要是为了给单元测试使用，除非了解运行原理，否则勿设置该参数
     * @param int $labelSize
     * @return $this
     */
    public function __setLabelSize(int $labelSize)
    {
        $this->labelSize = max(10, min(0x7FFFFFFF, $labelSize));
        return $this;
    }

    /**
     * 设置队列文件夹数据保存的文件夹路径
     * @param string $folder
     * @return $this
     */
    public function setFolder(string $folder)
    {
        $this->folder = $folder;
        return $this;
    }

    /**
     * 获取当前队列文件夹数据保存的文件夹路径
     * @return string
     */
    public function getFolder()
    {
        return $this->folder;
    }

    /**
     * 设置单个数据分片允许的最大文件尺寸（单位：MB），最大可设置为 2000
     * @param int $partitionSize
     * @return $this
     */
    public function setPartitionSize(int $partitionSize)
    {
        $this->partitionSize = max(1, min(2000, $partitionSize));
        return $this;
    }

    /**
     * 获取单个数据分片允许的最大文件尺寸（单位：MB）
     * @return int
     */
    public function getPartitionSize()
    {
        return $this->partitionSize;
    }

    /**
     * 添加一条消息到队列中
     * @param string $message 消息内容
     * @param int $delay 延迟时间（单位：秒）
     * @param string $topic 所属主题
     * @return $this
     */
    public function push(string $message, int $delay = 0, string $topic = self::DEFAULT_TOPIC)
    {
        $store = $delay ? ['delay' => [[$delay, $message]]] : ['queue' => [$message]];
        return $this->storeTopicMessage([$topic => $store]);
    }

    /**
     * 缓存一条消息，可缓存多条消息后，一次性使用 send 添加到队列中
     * @param string $message 消息内容
     * @param int $delay 延迟时间（单位：秒）
     * @param string $topic 所属主题
     * @return $this
     */
    public function setMessage(string $message, int $delay = 0, string $topic = self::DEFAULT_TOPIC)
    {
        if (!isset($this->messageStore[$topic])) {
            $this->messageStore[$topic] = [];
        }
        if ($delay) {
            if (!isset($this->messageStore[$topic]['delay'])) {
                $this->messageStore[$topic]['delay'] = [];
            }
            $this->messageStore[$topic]['delay'][] = [$delay, $message];
        } else {
            if (!isset($this->messageStore[$topic]['queue'])) {
                $this->messageStore[$topic]['queue'] = [];
            }
            $this->messageStore[$topic]['queue'][] = $message;
        }
        return $this;
    }

    /**
     * 将 setMessage 缓存的所有消息一次性添加到队列中
     * @param ?string $topic 仅发送指定 topic 主题的缓存消息，不指定则发送所有缓存消息
     * @return $this
     */
    public function send(string $topic = null)
    {
        if ($topic) {
            if (isset($this->messageStore[$topic])) {
                $topics = $this->messageStore[$topic];
                unset($this->messageStore[$topic]);
            } else {
                $topics = [];
            }
        } else {
            $topics = $this->messageStore;
            $this->messageStore = [];
        }
        return $this->storeTopicMessage($topics);
    }

    /**
     * 保存消息
     * @param array $topics
     * @return $this
     */
    private function storeTopicMessage(array $topics)
    {
        if (!count($topics)) {
            return $this;
        }
        $this->ignoreError();
        foreach ($topics as $topic => $message) {
            if (isset($message['delay'])) {
                $this->storeTopicDelay($topic, $message['delay']);
            } else {
                $this->storeTopicDelay($topic, []);
            }
            if (isset($message['queue'])) {
                $this->storeTopicQueue($topic, $message['queue']);
            }
        }
        $this->revertError();
        return $this;
    }

    /**
     * 保存延迟消息
     * @param string $topic
     * @param array $messages
     * @return $this
     */
    private function storeTopicDelay(string $topic, array $messages)
    {
        if (!count($messages)) {
            return $this;
        }
        $topicPath = $this->getTopicPath($topic, true);

        // 若刚好在重建延迟消息, 等待重建进程结束
        $time = 1000;
        while (true) {
            clearstatcache();
            if (!is_file($rebuildPath = $topicPath. '/delayRebuild')) {
                break;
            }
            // 关闭 delayMessage 指针给重建进程一个时间窗口
            $this->closeWriteHandler($topic, self::HANDLER_DELAY_MESSAGE);
            usleep($time);
            $time <<= 1;
            if ($time > 1000 << 13) {
                throw new CreateFailedException(sprintf('[%s] task is rebuilding [%s]', $topic, $rebuildPath));
            }
        }
        // 写入延迟消息
        $path =  $topicPath . '/delayMessage';
        $fp = $this->getWriteHandler($topic, self::HANDLER_DELAY_MESSAGE, $path, 'ab');
        if (!$fp || !flock($fp, LOCK_EX) || fseek($fp, 0, SEEK_END) < 0 || false === $offset = ftell($fp)) {
            if ($fp) {
                flock($fp, LOCK_UN);
            }
            throw new IoException(sprintf('Failed to open delay message [%s]', $path));
        }
        // 若是首次写入，开头写入 int(4) 作为 第一条延迟消息的位置信息
        $pack = 0 === $offset ? pack('L', 4) : '';
        $now = time();
        foreach ($messages as $message) {
            $time = $now + $message[0];
            $len = strlen($message[1]);
            $pack .= pack('LLa'.$len, $time, $len, $message[1]);
        }
        if (!fwrite($fp, $pack) || !flock($fp, LOCK_UN)) {
            throw new IoException(sprintf('Failed to write delay message [%s]', $path));
        }
        return $this;
    }

    /**
     * 保存队列消息
     * @param string $topic
     * @param array $messages
     * @param int $tryTimes
     * @return $this
     */
    private function storeTopicQueue(string $topic, array $messages, int $tryTimes = 0)
    {
        try {
            return $this->writeTopicQueue($topic, $messages);
        } catch (FileErrorException $e) {
            // 分片文件可能被其他进程以独占方式读写，尝试多次仍未分配到权限，抛出异常
            if ($tryTimes > 100) {
                throw $e;
            }
            $this->closeWriteHandler($topic);
            usleep(1000);
            return $this->storeTopicQueue($topic, $messages, $tryTimes + 1);
        }
    }

    /**
     * 将队列消息存储到分片文件中
     * @param string $topic
     * @param array $messages
     * @return $this
     */
    private function writeTopicQueue(string $topic, array $messages)
    {
        $name = $this->getCurrentPartition($topic, count($messages));
        $datFile = $name.'.dat';
        $indexFile = $name.'.index';

        // 打开 index 分片文件，获取偏移量（即 size 大小）
        $index = $this->getWriteHandler($topic, self::HANDLER_INDEX, $indexFile, 'ab');
        if (!$index || !flock($index, LOCK_EX) || fseek($index, 0, SEEK_END) < 0 ||
            false === ($size = ftell($index)) || 0 !== $size % 4
        ) {
            if ($index) {
                flock($index, LOCK_UN);
            }
            throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
        }
        // 打开 dat 分片文件，获取偏移量（即 size 大小）
        $dat = $this->getWriteHandler($topic, self::HANDLER_DAT, $datFile, 'ab');
        if (!$dat || !flock($dat, LOCK_EX) || fseek($dat, 0, SEEK_END) < 0 || false === $offset = ftell($dat)) {
            if ($dat) {
                flock($dat, LOCK_UN);
            }
            throw new FileErrorException(sprintf('Dat file [%s] is error', $datFile));
        }
        // 准备写入数据
        $datPack = '';
        $indexPack = '';
        $length = $offset;
        $increment = (int) ltrim(basename($name), '0') + $size / 4;
        foreach ($messages as $message) {
            $crc = self::crc($message);
            $len = strlen($message);
            $now = time();
            $length = $length + 16 + $len;
            $indexPack .= pack('l', $length);
            $datPack .= pack('lllla'.$len, $increment, $crc, $len, $now, $message);
            $increment++;
        }
        // 写入消息内容到 dat 分片中
        if (false === fwrite($dat, $datPack) || !flock($dat, LOCK_UN)) {
            throw new FileErrorException(sprintf('Dat file [%s] is error', $datFile));
        }
        // 写入消息位置到 index 分片中
        if (false === fwrite($index, $indexPack) || !flock($index, LOCK_UN)) {
            ftruncate($dat, $offset);
            throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
        }
        return $this;
    }

    /**
     * 获得 topic 当前分片文件的路径名
     * @param string $topic
     * @param int $padCount
     * @return string
     */
    private function getCurrentPartition(string $topic, int $padCount = 1)
    {
        $topicPath = $this->getTopicPath($topic, true);
        $lockFile = $topicPath . '/lock';

        // 若当前刚好 label 写满，其他进程正在转移分片文件，等待至转移完成
        $tryTimes = 0;
        while (true) {
            if (!is_file($lockFile)) {
                break;
            }
            if (++$tryTimes > 500) {
                throw new CreateFailedException(sprintf('[%s] task is locked by file [%s]', $topic, $lockFile));
            }
            usleep(10000);
            clearstatcache();
        }

        // 不存在索引文件，说明是首次写入，直接返回首个 分片路径名
        $path = $topicPath . '/' . self::HANDLER_PARTITION_INDEX;
        if (!is_file($path)) {
            if (!file_put_contents($path, pack('l', 0))) {
                throw new FileErrorException(sprintf('Write file [%s] failed', $path));
            }
            return $topicPath . '/' . str_pad(0, 10, '0', STR_PAD_LEFT);
        }

        // 从索引文件获取当前 分片路径名
        $partitionIndex = $this->getWriteHandler($topic, self::HANDLER_PARTITION_INDEX, $path, 'r+b');
        if (!$partitionIndex || !flock($partitionIndex, LOCK_EX)) {
            throw new FileErrorException(sprintf('Index file [%s] is error', $path));
        }
        if (fseek($partitionIndex, -4, SEEK_END) < 0 || false === $current = self::getPackInt($partitionIndex)) {
            flock($partitionIndex, LOCK_UN);
            throw new FileErrorException(sprintf('Index file [%s] is error', $path));
        }

        // 获取到当前 分片路径名，但分片文件不存在，直接返回
        $currentPath = $topicPath . '/' . str_pad($current, 10, '0', STR_PAD_LEFT);
        $datFile = $currentPath . '.dat';
        if (!is_file($datFile)) {
            flock($partitionIndex, LOCK_UN);
            return $currentPath;
        }

        // 获取当前 dat 中末尾消息的序号
        $indexFile = $currentPath . '.index';
        if (!is_file($indexFile)) {
            flock($partitionIndex, LOCK_UN);
            throw new FileErrorException(sprintf('Index file [%s] not exist', $indexFile));
        }
        $size = filesize($indexFile);
        if (0 !== $size % 4) {
            flock($partitionIndex, LOCK_UN);
            throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
        }
        $current += $size / 4;

        // 若消息序号已达到最大值，当前 label 存满, 创建 lock 文件做标记, 并获取下一个 label
        if ($current + $padCount > $this->labelSize) {
            flock($partitionIndex, LOCK_UN);
            touch($lockFile);


            clearstatcache();
            return $this->getCurrentPartition($topic, $padCount, $this->getPackIntByPath($labelFile, true) + 1);
        }






        // 该 label 已写满，获取下一个可用 label 文件夹
        $labelFile = $topicPath . '/label';
        if (is_file($lockFile)) {
            return $this->getCurrentPartition($topic, $padCount, $this->getPackIntByPath($labelFile, true) + 1);
        }











        // 当前 label 未存满，但当前 dat 已达最大文件体积, 使用新的 dat, 并记录新 dat 路径名到索引文件
        if (filesize($datFile) > $this->partitionSize << 20) {
            if (!fwrite($partitionIndex, pack('l', $current))) {
                throw new FileErrorException(sprintf('Write file [%s] failed', $path));
            }
            $currentPath = $topicPath . '/' . str_pad($current, 10, '0', STR_PAD_LEFT);
        }
        flock($partitionIndex, LOCK_UN);
        return $currentPath;
    }

    /**
     * 获取 topic 主题文件夹路径
     * @param string $topic
     * @param bool $create
     * @return string|false
     */
    private function getTopicPath(string $topic, bool $create = false)
    {
        clearstatcache();
        if (!is_dir($path = $this->folder . '/' . $topic)) {
            if (!$create) {
                return false;
            }
            if (false === mkdir($path, 0777, true)) {
                throw new CreateFailedException(sprintf('Unable to create queue directory [%s]', $path));
            }
        }
        return $path;

        // v(label总数) / v(当前正消费的label) / L(最后消费的序号)
//        if (false === mkdir($path, 0777, true) || !file_put_contents($path.'/current', pack('vvL', 0, 0, 0))) {
//            throw new CreateFailedException(sprintf('Unable to create queue directory [%s]', $path));
//        }
//        if (!$check) {
//            return $path;
//        }
//        // 若当前刚好 label 写满，正在转移分片文件，等待至转移完成
//        $tryTimes = 0;
//        while (true) {
//            if (!is_file($lockFile = $path . '/lock')) {
//                break;
//            }
//            if (++$tryTimes > 500) {
//                throw new CreateFailedException(sprintf('[%s] task is locked by file [%s]', $topic, $lockFile));
//            }
//            usleep(10000);
//            clearstatcache();
//        }
//        return $path;
    }















    /**
     * 由指定的起始序号和数量，获取一组消息队列
     * > 仅仅是获取消息：已消费的消息也会获取，未消费的消息在获取后也不会标记为已消费
     * @param int $offset
     * @param int $limit
     * @param string $topic
     * @return array
     */
    public function getQueue(int $offset = 0, int $limit = 1, string $topic = self::DEFAULT_TOPIC)
    {
        clearstatcache();
        $partition = $this->getTopicPath($topic);
        if (!$partition || !($size = $this->pushDelayMessage($topic)->getPartitionIndexSize($topic))) {
            return [];
        }
        $max = $size / 4;
        $start = $name = false;
        $partition = $partition . '/partitionIndex';

        // 二分法查找 offset 所在的 索引序号($start) 分片路径名($name)
        if ($offset) {
            $fp = $this->getReadHandler($topic, self::HANDLER_PARTITION_INDEX, $partition, 'rb');
            $low = 0;
            $high = $max;
            while ($low <= $high) {
                $mid = floor(($low + $high) / 2);
                $index = $this->getPartitionName($fp, $topic, $partition, $mid);
                if ($index === $offset) {
                    $start = $mid;
                    $name = $index;
                    break;
                } elseif ($index > $offset) {
                    $high = $mid - 1;
                } else {
                    if ($mid >= $max - 1) {
                        $start = $mid;
                        $name = $index;
                        break;
                    }
                    $nextIndex = $this->getPartitionName($fp, $topic, $partition, $mid + 1);
                    if ($nextIndex > $offset) {
                        $start = $mid;
                        $name = $index;
                        break;
                    } else {
                        $low = $mid + 1;
                    }
                }
            }
        } else {
            $fp = false;
            $start = $name = 0;
        }
        if (false === $name) {
            return [];
        }

        // 获取消息列表
        $count = 0;
        $messages = [];
        while (true) {
            $partitionMessage = $this->getPartitionMessage($name, $offset, $limit, $topic);
            $partitionCount = count($partitionMessage);
            if ($partitionCount) {
                $messages = array_merge($messages, $partitionMessage);
                $count += $partitionCount;
                if ($count >= $limit) {
                    break;
                }
                $start++;
                if ($start >= $max) {
                    break;
                }
                if (!$fp) {
                    $fp = $this->getReadHandler($topic, self::HANDLER_PARTITION_INDEX, $partition, 'rb');
                }
                $name = $this->getPartitionName($fp, $topic, $partition, $start);
                $offset = $name;
                $limit = $limit - $count;
            } else {
                break;
            }
        }
        return $messages;
    }


    /**
     * 将已到时间的延迟消息 写入到 队列中
     * @param string $topic
     * @return $this
     */
    private function pushDelayMessage(string $topic = self::DEFAULT_TOPIC)
    {
        if (!($folder = $this->getTopicPath($topic)) || !is_file($path = $folder . '/delayMessage')) {
            return $this;
        }

        // 打开存储延迟消息的文件
        $delay = $this->getReadHandler($topic, self::HANDLER_DELAY_MESSAGE, $path, 'r+b');
        if (!$delay || fseek($delay, 0, SEEK_SET) < 0 || false === $seek = $this->getPackInt($delay)) {
            throw new IoException(sprintf('Delay message file [%s] is error', $path));
        }
        flock($delay, LOCK_EX);
        fseek($delay, $seek, SEEK_SET);

        // 若 delayMessage 中的无效数据达到 partitionSize，在午夜对其进行重建
        if ($seek > $this->partitionSize << 20 && (date('G') < 2 || date('G') > 6)) {
            return $this->rebuildDelayMessage($delay, $path, $topic, $folder)->pushDelayMessage($topic);
        }

        // 保存未到时间延迟消息的临时文件
        $tempPath = $folder . '/delayTemp';
        $temp = fopen($tempPath, 'wb');
        flock($temp, LOCK_EX);

        // 上次处理延迟消息时，仍有遗留的未到时间的消息，先处理一下（到时间的写入队列，未到时间的写入到 delayTemp）
        $readPath = $folder . '/delayRead';
        if (is_file($readPath)) {
            $read = fopen($readPath, 'rb');
            flock($read, LOCK_EX);
            $this->writeDelayTemp($read, $temp, $topic, $readPath, $tempPath);
            flock($read, LOCK_UN);
            fclose($read);
            if (!unlink($readPath)) {
                throw new IoException(sprintf('Remove file [%s] failed', $readPath));
            }
        }

        // 处理当前 delayMessage，已到时间的消息写入到队列，未到时间的写入到 delayTemp 中
        $this->writeDelayTemp($delay, $temp, $topic, $path, $tempPath);

        // 写成功后，将 delayMessage 首个消息的位置改为当前偏移位置
        $seek = ftell($delay);
        if (fseek($delay, 0, SEEK_SET) < 0 || !fwrite($delay, pack('l', $seek))) {
            flock($delay, LOCK_UN);
            throw new IoException(sprintf('Write file [%s] failed', $path));
        }
        flock($delay, LOCK_UN);
        flock($temp, LOCK_UN);
        fclose($temp);
        usleep(100);

        // 若有未到时间的消息，修改 delayTemp 为 delayRead，否则删除 delayTemp
        if (filesize($tempPath)) {
            if (!rename($tempPath, $readPath)) {
                throw new IoException(sprintf('Rename file [%s] to [%s] failed', $tempPath, $readPath));
            }
        } elseif (!unlink($tempPath)) {
            throw new IoException(sprintf('Remove file [%s] failed', $tempPath));
        }
        return $this;
    }

    /**
     * 从 $read 中读取延迟消息，未到时间的写入到 $temp, 已到时间的写入到 topic 队列中
     * @param resource $read
     * @param resource $temp
     * @param string $topic
     * @param string $readPath
     * @param string $tempPath
     * @return $this
     */
    private function writeDelayTemp($read, $temp, string $topic, string $readPath, string $tempPath)
    {
        if (!is_resource($read) || !is_resource($temp)) {
            throw new IoException('Delay message handler is closed.');
        }
        $now = time();
        $messages = [];
        while (!feof($read)) {
            $header = fread($read, 8);
            if (empty($header) || !is_array($set = unpack('ltime/llen', $header))) {
                continue;
            }
            $len = (int) $set['len'];
            $content = fread($read, $len);
            if ($set['time'] > $now) {
                if (!fwrite($temp, $header . $content)) {
                    throw new IoException(sprintf('Failed to write delay message file [%s]', $tempPath));
                }
            } else {
                $message = unpack('a'.$len, $content);
                if (!is_array($message) || !isset($message[1])) {
                    throw new FileErrorException(sprintf('Delay message file [%s] is error', $readPath));
                }
                $messages[] = $message[1];
            }
        }
        if (count($messages)) {
            $this->storeTopicQueue($topic, $messages);
        }
        return $this;
    }

    /**
     * delayMessage 中已到时间的消息已写入到队列中，但并未从文件中移除，只是被标记为了无效数据，
     * 这里重建 delayMessage 文件，仅保留有效数据。
     * @param resource $delay delay message read handler
     * @param string $path delay message file path
     * @param string $topic
     * @param string $folder
     * @return $this
     */
    private function rebuildDelayMessage($delay, string $path, string $topic, string $folder)
    {
        // 在执行该函数前已经 seek 到有效数据的位置，仅需写入拷贝之后的数据即可
        $buildPath = $folder . '/delayRebuild';
        $build = fopen($buildPath, 'wb');
        if ($build) {
            fwrite($build, pack('l', 4));
            while (!feof($delay)) {
                fwrite($build, fread($delay, 65536));
            }
            fclose($build);
        }

        // 拷贝数据完成后，移除 delayMessage，并将 delayRebuild 修改为 delayMessage
        $rename = false;
        $this->closeWriteHandler($topic)->closeReadHandler($topic, self::HANDLER_DELAY_MESSAGE);
        for ($try = 0; $try < 100; $try++) {
            if (unlink($path) && rename($buildPath, $path)) {
                $rename = true;
                break;
            }
            $this->closeWriteHandler($topic);
            usleep(1000);
        }
        if (!$rename) {
            unlink($buildPath);
            throw new IoException(sprintf('Rebuild delay message file [%s] failed', $path));
        }
        return $this;
    }

    /**
     * 获取索引文件的大小
     * @param string $topic
     * @return bool|int
     */
    private function getPartitionIndexSize(string $topic)
    {
        if (!($path = $this->getTopicPath($topic))) {
            return 0;
        }
        // 获取索引文件的大小，若有问题则删除索引文件
        $size = false;
        $path = $path . '/partitionIndex';
        if (is_file($path)) {
            $size = filesize($path);
            if ($size % 4 !== 0) {
                if (!$this->removePartitionIndex($topic, $path)) {
                    throw new IoException(sprintf('Remove file [%s] failed.', $path));
                }
                $size = false;
            }
        }
        // 若索引文件不存或有问题，尝试重建索引文件
        if (!$size && false === $size = $this->repairPartitionIndex($path)) {
            throw new IoException(sprintf('Repair partition index file [%s] failed.', $path));
        }
        return $size;
    }

    /**
     * 删除索引文件
     * @param string $topic
     * @param string $path
     * @param int $tryTimes
     * @return bool
     */
    private function removePartitionIndex(string $topic, string $path, int $tryTimes = 1)
    {
        if (@unlink($path)) {
            return true;
        }
        if ($tryTimes > 10) {
            return false;
        }
        // partition index file may be used by write handler, try it several times
        if ($tryTimes < 2) {
            $this->closeWriteHandler($topic)->closeReadHandler($topic);
        } else {
            usleep(100);
        }
        return $this->removePartitionIndex($topic, $path, $tryTimes + 1);
    }

    /**
     * 重建索引文件
     * @param string $path
     * @return bool|int
     */
    private function repairPartitionIndex(string $path)
    {
        $partitions = $this->getPartitionList($path);
        if (!($count = count($partitions))) {
            return 0;
        }
        if (!($fp = fopen($path, 'wb')) || !flock($fp, LOCK_EX)) {
            return false;
        }
        foreach ($partitions as $partition) {
            $partition = (int) ltrim(basename($partition), '0');
            if (!fwrite($fp, pack('l', $partition))) {
                return false;
            }
        }
        if (!flock($fp, LOCK_UN) || !fclose($fp)) {
            return false;
        }
        return $count * 4;
    }

    /**
     * 获取所有的分片 index 文件名，以便重建索引
     * @param string $dir
     * @return array
     */
    private function getPartitionList(string $dir)
    {
        return self::getFolderList($dir, function($file) {
            if (substr($file, -6) === '.index') {
                return substr(basename($file), 0, -6);
            }
            return null;
        });
    }

    /**
     * 从 $fp 索引指针读取 offset 位置的 int 值
     * @param resource $fp
     * @param string $topic
     * @param string $partition
     * @param int $offset
     * @param int $tryTimes
     * @return false|int
     */
    private function getPartitionName($fp, string $topic, string $partition, int $offset, int $tryTimes = 0)
    {
        if (!$fp && (!$fp = $this->getReadHandler($topic, self::HANDLER_PARTITION_INDEX, $partition, 'rb'))) {
            throw new FileErrorException(sprintf('Open index file [%s] is failed', $partition));
        }
        // file may be locked by write handler, try it several times
        if (fseek($fp, $offset * 4, SEEK_SET) < 0 || false === $index = self::getPackInt($fp)) {
            if ($tryTimes > 500) {
                throw new FileErrorException(sprintf('Read index file [%s] failed', $partition));
            }
            usleep(1000);
            return $this->getPartitionName($fp, $topic, $partition, $offset, $tryTimes + 1);
        }
        return $index;
    }

    /**
     * 从分片文件中获取消息列表
     * @param int $start
     * @param int $offset
     * @param int $limit
     * @param string $topic
     * @param int $tryTimes
     * @return array
     */
    private function getPartitionMessage(
        int $start, int $offset, int $limit = 1, string $topic = self::DEFAULT_TOPIC, int $tryTimes = 0
    ) {
        if (!($path = $this->getTopicPath($topic))) {
            return [];
        }
        $path = $path . '/' . str_pad($start, 10, '0', STR_PAD_LEFT);
        $indexFile = $path . '.index';
        $size = @filesize($indexFile) ?: 0;
        if ($size % 4 !== 0) {
            throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
        }
        $size = $size / 4 - 1;
        if ($offset > $start + $size) {
            return [];
        }
        $datFile = $path . '.dat';
        if (!is_file($datFile) || !($dat = $this->getReadHandler($topic, self::HANDLER_DAT, $datFile, 'r'))) {
            throw new FileErrorException(sprintf('Data file [%s] not exist', $datFile));
        }
        if (!($index = $this->getReadHandler($topic, self::HANDLER_INDEX, $indexFile, 'r'))) {
            throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
        }
        if (!isset($this->partitionLabel[$topic])) {
            $this->partitionLabel[$topic] = $this->label($topic);
        }
        $label = $this->partitionLabel[$topic];
        $begin = $offset - $start;
        $end = min($size, $begin + $limit - 1);
        $messages = [];
        for ($position = $begin; $position <= $end; $position++) {

            // message position
            if ($position > 0) {
                $seek = ($position - 1) * 4;
                if (fseek($index, $seek, SEEK_SET) < 0 || false === $length = self::getPackInt($index)) {
                    // file may be locked by write handler, try it several times
                    if ($tryTimes < 500) {
                        usleep(1000);
                        return $this->getPartitionMessage($start, $offset, $limit, $topic, $tryTimes + 1);
                    }
                    throw new FileErrorException(sprintf('Index file [%s] is error', $indexFile));
                }
            } else {
                $length = 0;
            }

            // parse message
            $message = null;
            if (fseek($dat, $length, SEEK_SET) < 0 || !($message = fread($dat, 16))) {
                // file may be locked by write handler, try it several times
                if ($tryTimes < 500) {
                    usleep(1000);
                    return $this->getPartitionMessage($start, $offset, $limit, $topic, $tryTimes + 1);
                }
                throw new FileErrorException(sprintf('Data file [%s] is error', $datFile));
            }
            if (!is_array($message = unpack('loffset/lhash/llength/ltime', $message))) {
                throw new FileErrorException(sprintf('Data file [%s] is error', $datFile));
            }
            if ($message['offset'] !== $position + $start) {
                throw new FileErrorException(sprintf('Message offset not equal at data file [%s]', $datFile));
            }
            if (false !== $content = fread($dat, $message['length'])) {
                $content = unpack('a'.$message['length'], $content);
            }
            if (!is_array($content) || self::crc($content[1]) !== $message['hash']) {
                throw new FileErrorException(sprintf('Message hash not verified at data file [%s]', $datFile));
            }
            $message['message'] = $content[1];
            $message['label'] = $label;
            $messages[] = $message;
        }
        return $messages;
    }

    /**
     * 获取指定序号的消息, 消息不会被标记为已消费
     * @param int $offset
     * @param string $topic
     * @return array|null
     */
    public function getMessage(int $offset, string $topic = self::DEFAULT_TOPIC)
    {
        $queues = $this->getQueue($offset, 1, $topic);
        return count($queues) ? current($queues) : null;
    }

    /**
     * 出栈, 消费一条消息
     * @param string $topic 主题
     * @param bool $ignoreError 是否抑制错误，不抛出异常，返回 null
     * @return array|null
     * @throws Throwable
     */
    public function pop(string $topic = self::DEFAULT_TOPIC, bool $ignoreError = false)
    {
        $this->ignoreError();
        try {
            $message = $this->popMessage($topic);
        } catch (Throwable $e) {
            if ($ignoreError) {
                return null;
            }
            throw $e;
        }
        $this->revertError();
        return $message;
    }

    /**
     * 出栈一条消息
     * @param string $topic
     * @return array|null
     */
    private function popMessage(string $topic = self::DEFAULT_TOPIC)
    {
        if (!($topicPath = $this->getTopicPath($topic))) {
            return null;
        }
        $path = $topicPath . '/current';
        if (!is_file($path)) {
            touch($path);
        }
        $fp = $this->getReadHandler($topic, self::HANDLER_CURRENT, $path, 'r+b');
        flock($fp, LOCK_EX);
        fseek($fp, 0, SEEK_SET);
        $current = fread($fp, 4);
        if (empty($current)) {
            $current = 0;
        } else {
            $current = @unpack('l', $current);
            if (!is_array($current) || !isset($current[1])) {
                throw new FileErrorException(sprintf('Index file [%s] is error', $path));
            }
            $current = $current[1];
        }
        $message = $this->getMessage($current, $topic);
        if (!$message) {
            flock($fp, LOCK_UN);
            usleep(100);
            return $this->popFromNewStore($topic, $topicPath);
        }
        rewind($fp);
        ftruncate($fp, 0);
        fwrite($fp, pack('l', $current + 1));
        flock($fp, LOCK_UN);
        return $message;
    }

    /**
     * check if has new queue folder, and pop from this folder
     * @param string $topic
     * @param string $topicPath
     * @return null
     */
    private function popFromNewStore(string $topic, string $topicPath)
    {
        if (!is_file($topicPath . '/lock')) {
            return null;
        }
        $new = $this->getPackIntByPath($topicPath . '/label') + 1;

        // touch lock file
        $lockFile = $this->folder . '/' . $topic . '.lock';
        touch($lockFile);

        // close write and read handler
        $tryTimes = 0;
        while (true) {
            try {
                $this->closeWriteHandler($topic)->closeReadHandler($topic);
                $change = $this->changeTopicStore($topicPath, $new);
                unlink($lockFile);
                if ($change) {
                    unset($this->partitionLabel[$topic]);
                    return $this->popMessage($topic);
                }
                break;
            } catch (IoException $e) {
                if ($tryTimes > 100) {
                    unlink($lockFile);
                    throw $e;
                }
                clearstatcache();
                usleep(10000);
                $tryTimes++;
            }
        }
        return null;
    }

    /**
     * @param string $topicPath
     * @param int $new
     * @return bool
     */
    private function changeTopicStore(string $topicPath, int $new)
    {
        if (!is_dir($newPath = $topicPath.'_'.$new)) {
            return false;
        }
        $newPath = realpath($newPath);

        // rename current store folder
        $backup = $topicPath . '_h_'.$new;
        if (!rename($topicPath, $backup)) {
            throw new IoException(sprintf('Failed rename folder [%s] to [%s]', $topicPath, $backup));
        }

        // rename delay message file
        $oldDelay = $backup . DIRECTORY_SEPARATOR .'delayMessage';
        $newDelay = $newPath. DIRECTORY_SEPARATOR .'delayMessage';
        $hasDelay = is_file($oldDelay);
        if ($hasDelay && !rename($oldDelay, $newDelay)) {
            rename($backup, $topicPath);
            throw new IoException(sprintf('Failed rename file [%s] to [%s]', $oldDelay, $newDelay));
        }

        // rename delay read file
        $oldRead = $backup . DIRECTORY_SEPARATOR . 'delayRead';
        $newRead = $newPath . DIRECTORY_SEPARATOR . 'delayRead';
        $hasRead = is_file($oldRead);
        if ($hasRead && !rename($oldRead, $newRead)) {
            if ($hasDelay) {
                rename($newDelay, $oldDelay);
            }
            rename($backup, $topicPath);
            throw new IoException(sprintf('Failed rename file [%s] to [%s]', $oldRead, $newRead));
        }
        // rename new store
        if (!rename($newPath, $topicPath)) {
            if ($hasDelay) {
                rename($newDelay, $oldDelay);
            }
            if ($hasRead) {
                rename($newRead, $oldRead);
            }
            rename($backup, $topicPath);
            throw new IoException(sprintf('Failed rename folder [%s] to [%s]', $newPath, $topicPath));
        }
        return true;
    }












    /**
     * 获取指定主题当前的 label
     * > 一个文件夹下最大存储数据为 labelSize 条，消费后的数据不会被删除。
     * > 当消息总数超出 labelSize，会创建新的文件夹继续存储消息，当前 label 消费完毕后，消费下一个 label 下的消息
     * @param string $topic
     * @return int
     */
    public function label(string $topic = self::DEFAULT_TOPIC)
    {
        if (!($topicPath = $this->getTopicPath($topic)) || !is_file($labelFile = $topicPath . '/label')) {
            return 0;
        }
        return max(0, $this->getPackIntByPath($labelFile));
    }

    /**
     * 获取指定主题当前最大的消息序号
     * - 默认为当前 label 下的消息序号
     * - 若 fromStart=true 则获取从初始化到当前的序号，即 当前序号 + $this->label() * labelSize
     * @param string $topic 指定主题
     * @param bool $fromStart 是否从初始化开始
     * @return int|float
     */
    public function maxOffset(string $topic = self::DEFAULT_TOPIC, bool $fromStart = false)
    {
        if (!($topicPath = $this->getTopicPath($topic))) {
            return 0;
        }
        $this->pushDelayMessage($topic);
        if (!is_file($path = $topicPath . '/partitionIndex')) {
            return 0;
        }
        $cache = $this->getReadHandler($topic, self::HANDLER_PARTITION_INDEX, $path, 'rb');
        if (!$cache || fseek($cache, -4, SEEK_END) < 0 || false === $offset = self::getPackInt($cache)) {
            throw new FileErrorException(sprintf('Index file [%s] is error', $path));
        }
        $indexFile = $topicPath . '/' . str_pad($offset, 10, '0', STR_PAD_LEFT) . '.index';
        if (!is_file($indexFile) || ($size = filesize($indexFile)) % 4 !== 0) {
            throw new FileErrorException(sprintf('Index file [%s] not exist', $indexFile));
        }
        $offset = (int) $offset + $size / 4;
        if (!$fromStart || !is_file($labelFile = $topicPath . '/label')) {
            return $offset;
        }
        return $offset + $this->labelSize * max(0, $this->getPackIntByPath($labelFile));
    }

    /**
     * 获取指定主题当前未消费消息的开始序号
     * - 默认为当前 label 下的消息序号
     * - 若 fromStart=true 则获取从初始化到当前的序号，即 当前序号 + $this->label() * labelSize
     * @param string $topic 指定主题
     * @param bool $fromStart 是否从初始化开始
     * @return int|float
     */
    public function currentOffset(string $topic = self::DEFAULT_TOPIC, bool $fromStart = false)
    {
        if (!($topicPath = $this->getTopicPath($topic)) || !is_file($path = $topicPath.'/current')) {
            return 0;
        }
        $current = false;
        if (($fp = $this->getReadHandler($topic, self::HANDLER_CURRENT, $path, 'r+b')) && fseek($fp, 0, SEEK_SET) > -1) {
            $current = fread($fp, 4);
            $current = @unpack('l', $current);
        }
        if (!is_array($current) || !isset($current[1])) {
            throw new FileErrorException(sprintf('Current index file [%s] is error', $path));
        }
        if (!$fromStart || !is_file($labelFile = $topicPath . '/label')) {
            return $current[1];
        }
        return $current[1] + $this->labelSize * max(0, $this->getPackIntByPath($labelFile));
    }

    /**
     * 获取指定主题未消费的消息条数
     * @param string $topic 指定主题
     * @return int
     */
    public function length(string $topic = self::DEFAULT_TOPIC)
    {
        return max(0, $this->maxOffset($topic) - $this->currentOffset($topic));
    }










    /**
     * 获取 topic 下 handler 类型文件的 写入指针
     * @param string $topic 主题
     * @param string $handler 文件类型
     * @param string $path
     * @param string $flag
     * @return resource
     */
    private function getWriteHandler(string $topic, string $handler, string $path, string $flag)
    {
        return $this->getResourceHandler($topic, $handler, $path, $flag, false);
    }

    /**
     * 关闭队列的 写入指针
     * @param ?string $topic
     * @param ?string $handler
     * @return $this
     */
    private function closeWriteHandler(string $topic = null, string $handler = null)
    {
        $this->closeResourceHandler($topic, $handler, false);
        return $this;
    }

    /**
     * 获取 topic 下 handler 类型文件的 读取指针
     * @param string $topic
     * @param string $handler
     * @param string $path
     * @param string $flag
     * @return resource
     */
    private function getReadHandler(string $topic, string $handler, string $path, string $flag)
    {
        return $this->getResourceHandler($topic, $handler, $path, $flag, true);
    }

    /**
     * 关闭队列的 读取指针
     * @param ?string $topic
     * @param ?string $handler
     * @return $this
     */
    private function closeReadHandler(string $topic = null, string $handler = null)
    {
        $this->closeResourceHandler($topic, $handler, true);
        return $this;
    }

    /**
     * 获取指定条件的文件指针
     * @param string $topic
     * @param string $handler
     * @param string $flag
     * @param string $path
     * @param bool $read
     * @return resource|false
     */
    private function getResourceHandler(string $topic, string $handler, string $path, string $flag, bool $read = false)
    {
        $type = $read ? 'read' : 'write';
        if (isset($this->resourceHandler[$type])) {
            if (!isset($this->resourceHandler[$type][$topic])) {
                $this->resourceHandler[$type][$topic] = [];
            }
        } else {
            $this->resourceHandler[$type] = [];
            $this->resourceHandler[$type][$topic] = [];
        }
        $hash = md5($path.$flag);
        if (isset($this->resourceHandler[$type][$topic][$handler])) {
            if (count($this->resourceHandler[$type][$topic][$handler]) === 2) {
                if ($this->resourceHandler[$type][$topic][$handler][0] === $hash &&
                    $this->resourceHandler[$type][$topic][$handler][1]
                ) {
                    return $this->resourceHandler[$type][$topic][$handler][1];
                }
                @fclose($this->resourceHandler[$type][$topic][$handler][1]);
            }
            unset($this->resourceHandler[$type][$topic][$handler]);
        }
        if ($fp = fopen($path, $flag)) {
            $this->resourceHandler[$type][$topic][$handler] = [$hash, $fp];
        }
        return $fp;
    }

    /**
     * 关闭指定条件的文件指针
     * @param ?string $topic
     * @param bool $read
     * @param ?string $handler
     * @return bool
     */
    private function closeResourceHandler(string $topic = null, string $handler = null, bool $read = false)
    {
        $type = $read ? 'read' : 'write';
        if (!isset($this->resourceHandler[$type])) {
            return true;
        }
        foreach ($this->resourceHandler[$type] as $topicName => $steam) {
            if ($topic && $topic !== $topicName) {
                continue;
            }
            foreach ($steam as $handlerName => $item) {
                if ($handler && $handlerName !== $handler) {
                    continue;
                }
                @flock($item[1], LOCK_UN);
                @fclose($item[1]);
                if ($handler) {
                    unset($this->resourceHandler[$type][$topicName][$handlerName]);
                }
            }
            if (!$handler || !count($this->resourceHandler[$type][$topicName])) {
                unset($this->resourceHandler[$type][$topicName]);
            }
        }
        return true;
    }

    /**
     * 忽略警告消息
     */
    private function ignoreError()
    {
        if (null === $this->errorLever) {
            $this->errorLever = error_reporting();
            error_reporting($this->errorLever & ~E_NOTICE & ~E_WARNING);
        }
    }

    /**
     * 复原 php error 报告等级
     */
    private function revertError()
    {
        if (null !== $this->errorLever) {
            error_reporting($this->errorLever);
            $this->errorLever = null;
        }
    }

    /**
     * 在 $fp 指针中读取 4 字节并 unpack 为 int
     * @param resource $fp
     * @return int|false
     */
    private static function getPackInt($fp)
    {
        if ($index = $fp ? fread($fp, 4) : false) {
            $index = @unpack('l', $index);
            if (is_array($index) && isset($index[1])) {
                return $index[1];
            }
        }
        return false;
    }

    /**
     * 读取文件头部 4字节 int
     * @param string $path
     * @param bool $check
     * @return int
     */
    private static function getPackIntByPath(string $path, bool $check = false)
    {
        $handler = !$check || is_file($path) ? fopen($path, 'r') : false;
        if (!$handler || false === $int = self::getPackInt($handler)) {
            throw new FileErrorException(sprintf('Record file [%s] is error', $path));
        }
        fclose($handler);
        return (int) $int;
    }

    /**
     * 获取指定文件夹下的文件列表
     * @param string $dir
     * @param callable|null $filter
     * @return array
     */
    private static function getFolderList(string $dir, callable $filter = null)
    {
        $names = [];
        if (function_exists('glob')) {
            foreach (glob(realpath($dir) . DIRECTORY_SEPARATOR . '*') as $file) {
                if ($file = $filter ? $filter($file) : $file) {
                    $names[] = $file;
                }
            }
        } elseif ($handle = opendir($dir)) {
            while (false !== $file = readdir($handle)) {
                if ('.' === $file || '..' === $file) {
                    continue;
                }
                if ($file = $filter ? $filter($file) : $file) {
                    $names[] = $file;
                }
            }
            closedir($handle);
        }
        return $names;
    }

    /**
     * returns the same int value on a 64 bit mc. like the crc32() function on a 32 bit mc.
     * @param string $str
     * @return int
     */
    private static function crc(string $str)
    {
        $crc = crc32($str);
        if($crc & 0x80000000){
            $crc ^= 0xffffffff;
            $crc += 1;
            $crc = -$crc;
        }
        return $crc;
    }
}
