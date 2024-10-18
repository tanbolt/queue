<?php
namespace Tanbolt\Queue;

use Closure;
use Exception;
use Throwable;
use Tanbolt\Queue\Exception\SystemException;
use Tanbolt\Queue\Exception\MaxTryException;
use Tanbolt\Queue\Exception\PidFileException;
use Tanbolt\Queue\Exception\ReleaseException;
use Tanbolt\Queue\Exception\InvalidJobException;

class Worker
{
    // listener event
    const START = 'start';
    const ABORT = 'abort';

    const BEFORE = 'before';
    const AFTER = 'after';
    const FAILED = 'failed';

    // echo msg type
    const MSG_RUNNING = 0;
    const MSG_RUNNING_BACKGROUND = 1;
    const MSG_RUNNING_BACK_CONSOLE = 2;
    const MSG_JOB_TIMEOUT = 3;
    const MSG_MEMORY_EXCEEDED = 4;
    const MSG_STOPPING = 5;
    const MSG_STOPPED = 6;
    const MSG_STARTING = 7;
    const MSG_STARTED = 8;

    // executed job msg
    const MSG_EXECUTED_SUCCESS = 10;
    const MSG_EXECUTED_FAILED = 12;

    // stop/restart status
    const STOPPING = 1;
    const STOP_FORBID = 2;
    const STOP_TIMEOUT = 3;
    const STOP_FAILED = 4;
    const STOPPED = 10;
    const RESTARTED = 20;

    /**
     * @var int
     */
    private $daemon = 0;

    /**
     * @var string
     */
    private $pidFolder = null;

    /**
     * @var int
     */
    private $pid = null;

    /**
     * @var string
     */
    private $pidPrefix = 'queue_';

    /**
     * @var resource
     */
    private $pidMaster = null;

    /**
     * @var resource
     */
    private $pidHandler = null;

    /**
     * @var bool
     */
    private $pidSignal = null;

    /**
     * @var int
     */
    private $pidQuit = 0;

    /**
     * @var string
     */
    private $name = null;

    /**
     * @var array|null
     */
    private $connection = null;

    /**
     * @var string
     */
    private $topics = null;

    /**
     * @var int
     */
    private $timeout = 60;

    /**
     * @var int
     */
    private $maxMemory = 128;

    /**
     * @var int
     */
    private $maxTries = 0;

    /**
     * @var int
     */
    private $sleepTime = 3;

    /**
     * @var int
     */
    private $failedDelay = 0;

    /**
     * @var FifoAbstract|null
     */
    private $fifoHandler = null;

    /**
     * @var array
     */
    private $fifoTopics = null;

    /**
     * @var string
     */
    private $phpExecutable = false;

    /**
     * @var string
     */
    private $startFile = null;

    /**
     * @var bool
     */
    private $jobInExecuted = false;

    /**
     * @var string
     */
    private $currentJobName = null;

    /**
     * @var int
     */
    private $checkDaemonTime = 0;

    /**
     * @var JobAble|null
     */
    protected $consumer = null;

    /**
     * @var callable|null
     */
    protected $exceptionHandler = null;

    /**
     * @var callable|null
     */
    private $failedJobStoreHandler = null;

    /**
     * @var callable|null
     */
    protected $jobListener = null;

    /**
     * @var callable|null
     */
    protected $echoHandler = null;

    /**
     * 创建队列 Worker 对象
     * @param null $connect
     */
    public function __construct($connect = null)
    {
        if (!(php_sapi_name() == 'cli')) {
            throw new SystemException('Only run in command line mode.');
        }
        if ($connect) {
            $this->setConnection($connect);
        }
    }

    /**
     * 设置同时运行守护进程的数目
     * @param int $daemon
     * @return $this
     */
    public function setDaemon($daemon = 1)
    {
        $this->daemon = (int) $daemon;
        return $this;
    }

    /**
     * 获取同时运行守护进程的数目
     * @return int
     */
    public function getDaemon()
    {
        return $this->daemon;
    }

    /**
     * 设置守护运行的 pid 保存目录
     * @param string $pidFolder
     * @return $this
     */
    public function setPidFolder($pidFolder)
    {
        $this->pidFolder = $pidFolder;
        return $this;
    }

    /**
     * 获取守护运行的 pid 保存目录
     * @return string
     */
    public function getPidFolder()
    {
        if (!$this->pidFolder) {
            $this->pidFolder = __DIR__ . DIRECTORY_SEPARATOR . 'Pids';
        }
        return $this->pidFolder;
    }

    /**
     * 设置守护运行的 pid 文件名前缀
     * @param string $prefix
     * @return $this
     */
    public function setPidPrefix($prefix)
    {
        $this->pidPrefix = (string)$prefix;
        return $this;
    }

    /**
     * 获取守护运行的 pid 文件名前缀
     * @return string
     */
    public function getPidPrefix()
    {
        return $this->pidPrefix;
    }

    /**
     * 获取当前运行进程的 pid
     * @return int
     */
    public function getPid()
    {
        return $this->pid;
    }

    /**
     * 设置运行进程的名称
     * @param string $name
     * @return $this
     */
    public function setName($name)
    {
        $this->name = (string) $name;
        return $this;
    }

    /**
     * 获取运行进程的名称
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * 设置 Worker 连接队列的配置
     * 如果为字符串, 则通过 QueueResolver 查找具体连接信息
     * 如果为数组, 则认为就是连接信息
     * @param string|array $connection
     * @return $this
     */
    public function setConnection($connection)
    {
        $this->connection = $connection;
        $this->fifoHandler = $this->fifoTopics = null;
        return $this;
    }

    /**
     * 获取 Worker 连接队列的配置
     * @return array|string|null
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * 设置 Worker 要处理的主题, 可用数组设置多个
     * @param string|array $topics
     * @return $this
     */
    public function setTopics($topics)
    {
        $this->fifoTopics = null;
        $this->topics = is_array($topics) ? $topics : [(string) $topics];
        return $this;
    }

    /**
     * 获取当前 Worker 要处理的主题
     * @return string
     */
    public function getTopics()
    {
        return $this->topics;
    }

    /**
     * 设置当前 Worker 可使用的最大内存
     * @param int $maxMemory
     * @return $this
     */
    public function setMaxMemory($maxMemory)
    {
        $this->maxMemory = (int) $maxMemory;
        return $this;
    }

    /**
     * 获取当前 Worker 可使用的最大内存
     * @return int
     */
    public function getMaxMemory()
    {
        return $this->maxMemory;
    }

    /**
     * 设置 Worker 处理出栈消息的超时时长
     * @param int $timeout
     * @return $this
     */
    public function setTimeout($timeout)
    {
        $this->timeout = (int) $timeout;
        return $this;
    }

    /**
     * 获取 Worker 处理出栈消息的超时时长
     * @return int
     */
    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * 设置 Worker 处理出栈消息的最大尝试次数
     * @param int $maxTries
     * @return $this
     */
    public function setMaxTries($maxTries)
    {
        $this->maxTries = (int) $maxTries;
        return $this;
    }

    /**
     * 获取 Worker 处理出栈消息的最大尝试次数
     * @return int
     */
    public function getMaxTries()
    {
        return $this->maxTries;
    }

    /**
     * 处理消息失败后将重新入栈，此处设置入栈消息的延迟处理时长（秒）
     * 如果失败次数大于等于 setMaxTries 所设置的最大次数, 则由 setFailedJobStoreHandler 设置的回调函数, 或直接删除消息
     * @param int $delay
     * @return $this
     */
    public function setFailedDelay($delay)
    {
        $this->failedDelay = (int) $delay;
        return $this;
    }

    /**
     * 获取当前设置的失败消息重新入栈的延迟处理时长
     * @return int
     */
    public function getFailedDelay()
    {
        return $this->failedDelay;
    }

    /**
     * 设置 Worker 在处理完出栈消息后, 继续尝试获取消息的时间间隔
     * @param $sleepTime
     * @return $this
     */
    public function setSleepTime($sleepTime)
    {
        $this->sleepTime = (int) $sleepTime;
        return $this;
    }

    /**
     * 获取 Worker 在处理完出栈消息后, 继续尝试获取消息的时间间隔
     * @return int
     */
    public function getSleepTime()
    {
        return $this->sleepTime;
    }

    /**
     * 设置出栈消息的消费者
     * 一般情况下, 出栈消息反序列化后自带消费函数, 用不到这里设置的消费者
     * 但如果整个系统有其他生产者也在生产消息, 或者生产者发送的消息为纯文本, 就可以通过这里设置的消费者兜底
     * @param JobAble $consumer
     * @return $this
     */
    public function setConsumer(JobAble $consumer = null)
    {
        $this->consumer = $consumer;
        return $this;
    }

    /**
     * 设置出栈消息的监听函数
     * 进程启动 callback(self::START, Worker)
     * 消息被消费前 callback(self::BEFORE, JobAble)
     * 消息被消费后 callback(self::AFTER, JobAble)
     * 消费消息失败 callback(self::FAILED, FailedJob)
     * 进程意外退出 callback(self::ABORT, Worker)
     *
     * @param callable $jobListener
     * @return $this
     */
    public function setJobListener(callable $jobListener = null)
    {
        $this->jobListener = $jobListener;
        return $this;
    }

    /**
     * 设置守护进程 stdout 处理函数, 如未设置, 则使用 echo 输出字符串
     * @param callable $echoHandler
     * @return $this
     */
    public function setEchoHandler(callable $echoHandler = null)
    {
        $this->echoHandler = $echoHandler;
        return $this;
    }

    /**
     * 设置运行中的异常处理函数, 如未设置, 则忽略异常
     * @param callable $exceptionHandler
     * @return $this
     */
    public function setExceptionHandler(callable $exceptionHandler = null)
    {
        $this->exceptionHandler = $exceptionHandler;
        return $this;
    }

    /**
     * 当消息尝试失败多次（已达最大尝试次数），由此处设置的回调函数接手，若未设置，则直接删除消息（消息丢失）
     * @param callable $failedJobStoreHandler
     * @return $this
     */
    public function setFailedJobStoreHandler(callable $failedJobStoreHandler = null)
    {
        $this->failedJobStoreHandler = $failedJobStoreHandler;
        return $this;
    }

    /**
     * @param $trigger
     * @param null $data
     * @return $this
     */
    protected function triggerEvent($trigger, $data = null)
    {
        if (!$this->jobListener) {
            return $this;
        }
        call_user_func($this->jobListener, $trigger, $data);
        return $this;
    }

    /**
     * @param $msg
     * @param $type
     * @param bool $break
     * @return $this
     */
    protected function echoMsg($msg, $type, $break = true)
    {
        if ($this->echoHandler) {
            call_user_func($this->echoHandler, $msg, $type, $break);
        } else {
            echo $msg . ($break ? "\n" : '');
        }
        return $this;
    }

    /**
     * @param FailedJob $failedJob
     * @return $this
     */
    protected function storeFailedJob(FailedJob $failedJob)
    {
        if ($this->failedJobStoreHandler) {
            call_user_func($this->failedJobStoreHandler, $failedJob);
        }
        return $this;
    }

    /**
     * @param $e
     * @return $this
     */
    protected function triggerException($e)
    {
        if ($this->exceptionHandler) {
            call_user_func($this->exceptionHandler, $e);
        }
        return $this;
    }

    /**
     * @return $this
     */
    protected function preparedFifo()
    {
        if (!defined('SIGUSR1')) {
            define('SIGUSR1', 10);
        }
        if (!$this->fifoHandler || !$this->fifoTopics) {
            if (!$this->fifoHandler) {
                $this->fifoHandler = QueueResolver::getQueueDriverOrThrow($this->getConnection());
            }
            if (!$this->fifoTopics) {
                $conn = QueueResolver::getConnection($this->getConnection());
                $topics = $this->getTopics();
                if (!$topics) {
                    $topics = [isset($conn['topic']) && is_string($conn['topic']) ? $conn['topic'] : 'default'];
                }
                $this->fifoTopics = $topics;
            }
        }
        return $this;
    }

    /**
     * 仅出栈并消费一条消息
     * @param int $maxTries 失败尝试次数, 若未指定, 首先根据出栈消息的 maxTries 设置, 再次根据当前对象 setMaxTries 设置
     * @return JobMessage|null
     */
    public function consumeJob($maxTries = null)
    {
        $this->triggerEvent(self::START, $this);
        return $this->setDaemon(0)->preparedFifo()->tryConsumeJob(false, $maxTries);
    }

    /**
     * @param bool $inProcess
     * @param null $maxTries
     * @return JobMessage|null
     */
    protected function tryConsumeJob($inProcess = true, $maxTries = null)
    {
        // stop process memory exceeded
        if ($inProcess && $this->maxMemory && (memory_get_usage() / 1024 / 1024) >= $this->maxMemory) {
            $this->abortProcess(SIGUSR1);
        }
        if ($inProcess) {
            $this->watchIfDaemonDead();
        }
        // get last job
        if (!($jobMessage = $this->popJob())) {
            if (!$inProcess) {
                $this->echoMsg('There is no job to pop', self::MSG_EXECUTED_SUCCESS);
            }
            return null;
        }
        $this->setJobInExecuted(true);
        $job = @unserialize($jobMessage->payload);
        if ($job instanceof JobAble) {
            if ($maxTries === null) {
                $maxTries = $job->maxTries;
            }
        } else {
            $job = null;
        }
        try {
            if (!$job && $this->consumer) {
                $job = $this->consumer->reset();
            }
            if (!$job) {
                if (!$jobMessage->release(30)) {
                    throw new ReleaseException('Release buffer job failed');
                }
                if (!$inProcess) {
                    $this->echoMsg('No job to run', self::MSG_EXECUTED_SUCCESS);
                }
            } else {
                $this->executeJob($job, $jobMessage, $maxTries, $inProcess);
            }
            $this->setJobInExecuted(false);
        } catch (Exception $e) {
            $this->handleFailedJob($job, $jobMessage, $e, $maxTries);
        } catch (Throwable $e) {
            $this->handleFailedJob($job, $jobMessage, $e, $maxTries);
        }
        return $jobMessage;
    }

    /**
     * @param bool $inExecuted
     * @return $this
     */
    protected function setJobInExecuted($inExecuted = true)
    {
        $this->jobInExecuted = $inExecuted;
        return $this;
    }

    /**
     * @return JobMessage|null
     */
    protected function popJob()
    {
        foreach ($this->fifoTopics as $topic) {
            $jobMessage = $this->fifoHandler->pop($topic);
            if (!is_null($jobMessage)) {
                // this should be never happen, make sure fifo interface return JobMessage instance
                if (!$jobMessage instanceof JobMessage) {
                    throw new InvalidJobException('Data from queue not [' . __NAMESPACE__ . '\JobAble] instance.');
                }
                return $jobMessage;
            }
        }
        return null;
    }

    /**
     * @param JobAble $job
     * @param JobMessage $jobMessage
     * @param null $maxTries
     * @param bool $inProcess
     */
    protected function executeJob(JobAble $job, JobMessage $jobMessage, $maxTries = null, $inProcess = false)
    {
        // trigger before event
        $this->currentJobName = get_class($job);
        $this->triggerEvent(self::BEFORE, $this->installJobMessage($job, $jobMessage));
        if ($this->checkJobExceedsMaxTries($jobMessage, $maxTries, true)) {
            throw new MaxTryException('Job has been attempted too many times.');
        }
        // if in process and support signal, send alarm to process if timeout
        $alarm = false;
        if ($inProcess && $this->pidSignal && ($timeout = $job->timeout ?: $this->getTimeout())) {
            pcntl_alarm($timeout + $this->sleepTime);
            $alarm = true;
        }
        $job->consume();
        $job->delete();
        $this->incrementCounter(false, 1, $inProcess);
        if ($alarm) {
            pcntl_alarm(0);
        }
        $this->triggerEvent(self::AFTER, $job);
        if (!$this->getDaemon()) {
            $this->echoMsg(
                '['.date('Y-m-d H:i:s', time()).'] Success: '.$this->currentJobName,
                self::MSG_EXECUTED_SUCCESS
            );
        }
    }

    /**
     * @param JobAble $job
     * @param JobMessage $jobMessage
     * @return JobAble
     */
    protected function installJobMessage(JobAble $job, JobMessage $jobMessage)
    {
        return $job->setMessage($jobMessage)->setTopic($jobMessage->topic)->setConnection($this->getConnection());
    }

    /**
     * @param JobAble|null $job
     * @param JobMessage $jobMessage
     * @param $e
     * @param null $maxTries
     */
    protected function handleFailedJob($job, JobMessage $jobMessage, $e, $maxTries = null)
    {
        if (!$job instanceof JobAble) {
            throw $e;
        }
        try {
            $failedJob = new FailedJob($job, $e, $this->getName());
            $this->triggerEvent(self::FAILED, $failedJob);
            if ($this->checkJobExceedsMaxTries($jobMessage, $maxTries)) {
                $jobMessage->delete();
                $this->storeFailedJob($failedJob);
            } else {
                $this->tryRelease($jobMessage);
            }
        } catch (Exception $exception) {
            $this->tryRelease($jobMessage);
        } catch (Throwable $exception) {
            $this->tryRelease($jobMessage);
        }
        throw $e;
    }

    /**
     * if release failed, report exception
     * @param JobMessage $jobMessage
     * @return bool
     */
    protected function tryRelease(JobMessage $jobMessage)
    {
        return $this->tryToRun(function () use ($jobMessage) {
            $jobMessage->release($this->getFailedDelay());
        }, true);
    }

    /**
     * @param JobMessage $jobMessage
     * @param null $maxTries
     * @param bool $prepend
     * @return bool
     */
    protected function checkJobExceedsMaxTries(JobMessage $jobMessage, $maxTries = null, $prepend = false)
    {
        if ($maxTries === null) {
            $maxTries = $this->getMaxTries();
        }
        if ($maxTries <= 0) {
            return false;
        }
        $count = (int) $jobMessage->dequeueCount;
        return $prepend ? $count > $maxTries : $count >= $maxTries;
    }

    /**
     * increment results counter
     * @param bool $failed
     * @param int $increment
     * @param bool $inProcess
     * @return int
     */
    protected function incrementCounter($failed = false, $increment = 1, $inProcess = true)
    {
        if (!$inProcess) {
            return 0;
        }
        // handler 12bit: status isDaemon success failed
        $handler = $this->setPidHandler();
        fseek($handler, ($failed ? 12 : 8), SEEK_SET);
        $counter = fread($handler, 4);
        $counter = $counter ? unpack('l', $counter) : null;
        if (!is_array($counter) || !isset($counter[1])) {
            $this->pidHandler = null;
            return $this->incrementCounter($failed);
        }
        $counter = $counter[1] + $increment;
        fseek($handler, ($failed ? 12 : 8), SEEK_SET);
        fwrite($handler, pack('l', $counter));
        return (int)$counter;
    }

    /**
     * @return resource
     */
    protected function setPidHandler()
    {
        if (!$this->pidHandler) {
            $folder = $this->getPidFolderPrefixPath();
            $this->rebuildMasterFile(function () use ($folder) {
                $pidMaster = $this->getMasterHandler(null, $folder . 'pids');
                fseek($pidMaster, 0, SEEK_END);
                fwrite($pidMaster, pack('l', $this->pid));
            });
            $pidFile = $folder . $this->pid;
            if (!is_file($pidFile)) {
                touch($pidFile);
            }
            // set pidHandler
            $this->pidHandler = fopen($pidFile, 'r+b');
            $info = [
                'pid' => $this->pid,
                'name' => $this->getName(),
                'connectionName' => $this->fifoHandler->getName(),
                'connection' => $this->fifoHandler->getConnection(),
                'topics' => $this->fifoTopics,
                'maxMemory' => $this->getMaxMemory(),
                'maxTries' => $this->getMaxTries(),
                'timeout' => $this->getTimeout(),
                'sleepTime' => $this->getSleepTime(),
                'failedDelay' => $this->getFailedDelay(),
                'php' => $this->getPhpExecutable(),
                'cwd' => getcwd(),
                'startFile' => $this->startFile,
                'argv' => $_SERVER['argv'],
                'startTime' => time(),
            ];
            $info = serialize($info);
            ftruncate($this->pidHandler, 0);
            fseek($this->pidHandler, 0);
            // status isDaemon success failed
            fwrite($this->pidHandler, pack('llll', 0, ($this->getDaemon() ? 1 : 0), 0, 0));
            fwrite($this->pidHandler, $info);
        }
        return $this->pidHandler;
    }

    /**
     * @return bool
     * @throws Exception
     * @throws Throwable
     */
    protected function runAsDaemon()
    {
        // process is daemon now, return and delete temp file
        if (isset($GLOBALS['__TANBOLT__WORKER__DAEMON__FILE__'])) {
            @unlink($GLOBALS['__TANBOLT__WORKER__DAEMON__FILE__']);
            if (isset($GLOBALS['__TANBOLT__WORKER__START_FILE__']) &&
                isset($GLOBALS['__TANBOLT__WORKER__START_ARGV__']) &&
                isset($GLOBALS['__TANBOLT__WORKER__START_CWD__'])
            ) {
                chdir($GLOBALS['__TANBOLT__WORKER__START_CWD__']);
                $this->startFile = $GLOBALS['__TANBOLT__WORKER__START_FILE__'];
                $_SERVER['argv'] = $GLOBALS['__TANBOLT__WORKER__START_ARGV__'];
                $this->setDaemon(1);
                return true;
            } else {
                throw new SystemException('Run as daemon, start file not defined.');
            }
        }
        // check other daemon worker is not dead
        $this->watchIfDaemonDead();
        $daemonProcess = $this->getDaemon();
        if (!$daemonProcess) {
            return false;
        }
        $r = $this->runDaemon($this->getPhpExecutable(), $_SERVER['argv'], getcwd(), $this->startFile, $daemonProcess);
        if ($r === self::MSG_RUNNING_BACKGROUND) {
            $this->echoMsg('Success: worker is running in the background', self::MSG_RUNNING_BACKGROUND);
        } elseif ($r === self::MSG_RUNNING_BACK_CONSOLE) {
            $this->echoMsg(
                'Success: worker is running in the background, do not close this console window.',
                self::MSG_RUNNING_BACK_CONSOLE
            );
        }
        exit(0);
    }

    /**
     * fork new daemon by exec php command
     * @param $php
     * @param $argv
     * @param $cwd
     * @param $startFile
     * @param int $daemon
     * @return int
     * @throws Exception
     * @throws Throwable
     */
    protected function runDaemon($php, $argv, $cwd, $startFile, $daemon = 1)
    {
        if (!$php) {
            throw new SystemException("Can not found executable php.");
        }
        $args = [];
        $scriptFile = $scriptCode = null;
        foreach ((array)$argv as $v) {
            if ($scriptFile || $v[0] === '-') {
                $args[] = $v;
                continue;
            }
            $scriptFile = $this->getPidFolderPrefixPath(md5($startFile));
            $scriptCode =
                '<?php' . "\n" .
                '$__TANBOLT__WORKER__DAEMON__FILE__ = \'' . $scriptFile . '\';' . "\n" .
                '$__TANBOLT__WORKER__START_FILE__ = \'' . $startFile . '\';' . "\n" .
                '$__TANBOLT__WORKER__START_CWD__ = \'' . $cwd . '\';' . "\n" .
                '$__TANBOLT__WORKER__START_ARGV__ = ' . var_export($argv, true) . ';' . "\n" .
                'include(\'' . $startFile . '\');';
            $args[] = $scriptFile;
        }
        $args = join(' ', $args);
        if (!$scriptFile || !$scriptCode) {
            throw new SystemException('Could not open start file');
        }
        try {
            $r = null;
            $start = 0;
            while (true) {
                if (!file_put_contents($scriptFile, $scriptCode)) {
                    throw new SystemException('Could not create input file : ' . $scriptFile);
                }
                $r = DIRECTORY_SEPARATOR === '\\' ?
                    $this->runDaemonOnWin($php, $args) :
                    $this->runDaemonOnUnix($php, $args);
                if (++$start < $daemon) {
                    usleep(100000);
                } else {
                    break;
                }
            }
            return $r;
        } catch (Exception $e) {
            @unlink($scriptFile);
            throw $e;
        } catch (Throwable $e) {
            @unlink($scriptFile);
            throw $e;
        }
    }

    /**
     * try use WScript.Shell run php command on windows
     * @param $php
     * @param $args
     * @return int
     */
    protected function runDaemonOnWin($php, $args)
    {
        if (!class_exists('COM')) {
            return $this->runDaemonOnWinConsoleBackground($php, $args);
        }
        try {
            $command = sprintf('CMD /C %s %s >nul 2>&1', $php, $args);
            $WshShell = new \COM("WScript.Shell");
            $oExec = $WshShell->Run($command, 0, false);
            if ($oExec) {
                unset($WshShell);
                throw new SystemException("Run as daemon, create child process failed.");
            }
            unset($WshShell, $oExec);
            return self::MSG_RUNNING_BACKGROUND;
        } catch (Exception $e) {
            return $this->runDaemonOnWinConsoleBackground($php, $args);
        } catch (Throwable $e) {
            return $this->runDaemonOnWinConsoleBackground($php, $args);
        }
    }

    /**
     * use pclose(popen()) run php command on windows
     * @param $php
     * @param $args
     * @return int
     */
    protected function runDaemonOnWinConsoleBackground($php, $args)
    {
        if ($this->getDisabledFunction(['popen', 'pclose'], true) ||
            pclose(popen(sprintf('START /B %s %s >nul 2>&1', $php, $args), 'r')) < 0
        ) {
            throw new SystemException("Run as daemon, create child process failed.");
        }
        return self::MSG_RUNNING_BACK_CONSOLE;
    }

    /**
     * use exec run php command on unix
     * @param $php
     * @param $args
     * @return int
     */
    protected function runDaemonOnUnix($php, $args)
    {
        $code = $pid = null;
        if (!$this->getDisabledFunction(['exec'], true)) {
            exec(sprintf('%s %s >/dev/null 2>&1 & echo $!', $php, $args), $pid, $code);
        }
        if ($code || !$pid) {
            throw new SystemException("Run as daemon, create child process failed.");
        }
        return self::MSG_RUNNING_BACKGROUND;
    }

    /**
     * @return string
     */
    protected function getPhpExecutable()
    {
        if ($this->phpExecutable === false) {
            if (isset($_SERVER['_']) && is_executable($_SERVER['_'])) {
                $this->phpExecutable = $_SERVER['_'];
            } elseif (PHP_BINARY && is_executable(PHP_BINARY)) {
                $this->phpExecutable = PHP_BINARY;
            } elseif (($php = getenv('PHP_PATH')) && is_executable($php)) {
                $this->phpExecutable = $php;
            } elseif (($php = getenv('PHP_PEAR_PHP_BIN')) && is_executable($php)) {
                $this->phpExecutable = $php;
            } else {
                $this->phpExecutable = null;
            }
        }
        return $this->phpExecutable;
    }

    /**
     * @param array $functions
     * @param bool $throw
     * @return bool
     */
    protected function getDisabledFunction(array $functions, $throw = false)
    {
        foreach ($functions as $function) {
            if (!function_exists($function)) {
                if ($throw) {
                    throw new SystemException('Function [' . $function . '] not exists.');
                }
                return $function;
            }
        }
        return false;
    }

    /**
     * watch other process, if dead, restart
     * @return $this
     */
    protected function watchIfDaemonDead()
    {
        $time = time();
        $isLock = is_file($this->getPidFolderPrefixPath('lock'));
        if ($isLock || $time - $this->checkDaemonTime > 60) {
            $this->checkDaemonTime = $time;
            if (!$isLock) {
                $this->status();
            }
        }
        return $this;
    }

    /**
     * register alrm signal handler
     * @return $this
     */
    protected function registerSignals()
    {
        if (!$this->pidSignal) {
            return $this;
        }
        // use this, we can handle hup signal
        ignore_user_abort(true);
        // terminal close
        pcntl_signal(SIGHUP, function () {
            $this->pidQuit = SIGHUP;
        });
        // kill pid
        pcntl_signal(SIGTERM, function () {
            $this->pidQuit = SIGTERM;
        });
        // ctrl+c  or  kill -SIGINT pid
        pcntl_signal(SIGINT, function () {
            $this->pidQuit = SIGINT;
        });
        // ctrl+\ or kill -SIGQUIT pid
        pcntl_signal(SIGQUIT, function () {
            $this->pidQuit = SIGQUIT;
        });
        // normal exit
        pcntl_signal(SIGUSR2, function () {
            $this->pidQuit = SIGUSR2;
            if (!$this->jobInExecuted) {
                $this->stopIfNecessary();
            }
        });
        // timeout >> send alrm signal >> abort process
        pcntl_signal(SIGALRM, function () {
            $this->abortProcess(SIGALRM);
        });
        return $this;
    }

    /**
     * @param int $code
     * @return $this
     */
    protected function abortProcess($code = 0)
    {
        $this->triggerEvent(self::ABORT, $this);
        if ($code === SIGUSR1) {
            $this->echoMsg('memory exceeded limit [' . $this->maxMemory . 'M], worker is stop',
                self::MSG_MEMORY_EXCEEDED);
        } else {
            $this->echoMsg('time exceeded max limit, worker is stop', self::MSG_JOB_TIMEOUT);
        }
        exit($code);
    }

    /**
     * @return $this
     */
    protected function stopIfNecessary()
    {
        if ($this->pidSignal) {
            $quit = $this->pidQuit === SIGUSR2 ? true : $this->pidQuit;
        } else {
            // quit: status value > 1
            $quit = false;
            if ($this->pidHandler) {
                $pidFile = $this->getPidFolderPrefixPath($this->pid);
                fseek($this->pidHandler, 0, SEEK_SET);
                $status = fread($this->pidHandler, 4);
                $status = $status ? @unpack('l', $status) : null;
                if (!is_array($status) || !isset($status[1])) {
                    throw new PidFileException('Pid file is error : ' . $pidFile);
                }
                $quit = (bool)$status[1];
                if ($quit) {
                    fclose($this->pidHandler);
                    $this->pidHandler = null;
                }
            }
        }
        if ($quit) {
            $this->triggerEvent(self::ABORT, $this);
            // quit=true, worker stop command
            if ($quit === true) {
                // remove pid from master file
                $this->removeMasterPid($this->pid);
                exit(0);
            } else {
                exit($quit);
            }
        }
        return $this;
    }

    /**
     * @param $e
     * @return bool
     */
    protected function loggerException($e)
    {
        return $this->tryToRun(function() use ($e) {
            /** @var Exception|Throwable $e */
            $this->setJobInExecuted(false)->incrementCounter(true);
            $this->triggerException($e);
            if (!$this->getDaemon()) {
                if (!($name = $this->currentJobName)) {
                    $name = $e->getFile() . '('.$e->getLine().')';
                }
                $this->echoMsg('['.date('Y-m-d H:i:s', time()).'] Failed: '.$name, self::MSG_EXECUTED_FAILED);
                $this->currentJobName = null;
            }
        });
    }

    /**
     * @return JobMessage|null
     */
    protected function runNextJob()
    {
        try {
            return $this->stopIfNecessary()->tryConsumeJob();
        } catch (Exception $e) {
            $this->loggerException($e);
        } catch (Throwable $e) {
            $this->loggerException($e);
        }
        return true;
    }

    /**
     * 运行 queue worker
     */
    public function start()
    {
        $startFile = get_included_files();
        $this->startFile = array_shift($startFile);
        $isDaemon = $this->runAsDaemon();
        $this->triggerEvent(self::START, $this);
        $this->pid = getmypid();
        $this->pidSignal = !$this->getDisabledFunction(['pcntl_signal', 'pcntl_alarm', 'pcntl_signal_dispatch']);
        $this->preparedFifo()->registerSignals()->incrementCounter(false, 0);
        if (!$isDaemon) {
            $this->echoMsg('Worker is running...', self::MSG_RUNNING);
        }
        while (true) {
            if ($this->pidSignal) {
                pcntl_signal_dispatch();
            }
            if (!$this->runNextJob()) {
                sleep($this->sleepTime);
            }
        }
    }

    /**
     * 停止运行 queue worker
     * @param $pid
     * @param bool $force
     * @return array
     */
    public function stop($pid, $force = false)
    {
        return $this->endWorker($pid, $force);
    }

    /**
     * 重启 queue worker
     * @param $pid
     * @param bool $force
     * @return array
     */
    public function restart($pid, $force = false)
    {
        return $this->endWorker($pid, $force, true);
    }

    /**
     * @param $pid
     * @param bool $force
     * @param bool $restart
     * @return array
     */
    protected function endWorker($pid, $force = false, $restart = false)
    {
        $endResult = [];
        $status = $this->getAllStatus(false);
        $folder = $this->getPidFolderPrefixPath();
        if ($pid !=='all' && !is_array($pid)) {
            $pid = [$pid];
        }

        // change process status first
        $this->echoMsg('Stopping worker...', self::MSG_STOPPING, false);
        $supportPosixKill = function_exists('posix_kill');
        foreach ($status as $currentPid => $sta) {
            if (($pid !== 'all' && !in_array($currentPid, $pid))) {
                continue;
            }
            // if not a daemon process, forbidden restart
            if ($restart && !$sta['daemon']) {
                $endResult[$currentPid] = self::STOP_FORBID;
                continue;
            }
            if ($sta['dead']) {
                $endResult[$currentPid] = self::STOPPED;
            } else {
                $pidFile = $folder.$currentPid;
                $pidHandler = fopen($pidFile, 'r+b');
                fseek($pidHandler, 0, SEEK_SET);
                fwrite($pidHandler, pack('l', 1));
                fclose($pidHandler);
                $endResult[$currentPid] = self::STOPPING;
                if ($supportPosixKill) {
                    posix_kill($currentPid, SIGUSR2);
                } else {
                    usleep(10000);
                }
            }
        }
        if (!$force && !$restart) {
            $this->echoMsg('   done', self::MSG_STOPPED);
            return $endResult;
        }

        // wait normal exit, if timeout, force stop process
        $canKill = function_exists('exec');
        $forceKilled = [];
        usleep($hasWait = 100000);
        while (true) {
            $alive = 0;
            $forceStop = $hasWait >= 1000000 * 20;
            foreach ($endResult as $currentPid => $mode) {
                if ($mode !== self::STOPPING) {
                    continue;
                }
                if ($this->isWorkerDead($currentPid)) {
                    $endResult[$currentPid] = self::STOPPED;
                } elseif ($forceStop) {
                    if ($canKill) {
                        if ($this->kill($currentPid)) {
                            $forceKilled[] = $currentPid;
                            $endResult[$currentPid] = self::STOPPED;
                        } else {
                            $endResult[$currentPid] = self::STOP_FAILED;
                        }
                    } else {
                        $endResult[$currentPid] = self::STOP_TIMEOUT;
                    }
                } else {
                    $alive++;
                }
            }
            if (!$alive) {
                break;
            }
            $hasWait += ($sleep = 1000000);
            usleep($sleep);
        }
        if ($forceKilled) {
            $this->removeMasterPid($forceKilled);
        }
        $this->echoMsg('   done', self::MSG_STOPPED);
        if (!$restart) {
            return $endResult;
        }

        // restart daemon process
        $this->echoMsg('Starting worker...', self::MSG_STARTING, false);
        foreach ($endResult as $currentPid => $mode) {
            if ($mode !== self::STOPPED) {
                continue;
            }
            if ($this->rebootDeadWorker($status[$currentPid])) {
                $endResult[$currentPid] = self::RESTARTED;
                usleep(100000);
            }
        }
        $this->echoMsg('   done', self::MSG_STARTED);
        return $endResult;
    }

    /**
     * @param $pid
     * @return bool
     */
    protected function kill($pid)
    {
        if (DIRECTORY_SEPARATOR === '\\') {
            $command = 'TASKKILL /F /PID %d 1>nul 2>&1';
        } else {
            $command = 'kill -9 %d 1>/dev/null 2>&1';
        }
        exec(sprintf($command, (int) $pid), $output, $code);
        return $code === 0;
    }

    /**
     * 获取当期 queue worker 状态
     * @return array
     */
    public function status()
    {
        return $this->getAllStatus(true);
    }

    /**
     * @param bool $restartDeadDaemon
     * @return array
     */
    protected function getAllStatus($restartDeadDaemon = false)
    {
        $status = [];
        $newPid = '';
        $reboot = false;
        $rebuild = false;
        $folder = $this->getPidFolderPrefixPath();
        $pidMaster = $this->getMasterHandler(function($buffer, $pid) use (
            $restartDeadDaemon, $folder, &$newPid, &$reboot, &$rebuild, &$status
        ) {
            $processFile = $folder.$pid;
            if (!is_file($processFile)) {
                $rebuild = true;
                return false;
            }
            $info = $this->getPidInfo($processFile);
            $info['dead'] = $this->isWorkerDead($pid) ? 1 : 0;
            if ($info['dead']) {
                $rebuild = true;
                @unlink($processFile);
                if (!$info['daemon']) {
                    return false;
                }
                if (!$reboot) {
                    $reboot = true;
                }
            } else {
                $newPid .= $buffer;
            }
            $status[$pid] = $info;
            return false;
        }, $folder.'pids');

        // if changed, rebuild pid master file
        if ($rebuild) {
            $this->rebuildMasterFile(function() use ($pidMaster, $newPid){
                ftruncate($pidMaster, 0);
                fseek($pidMaster, 0);
                fwrite($pidMaster, $newPid);
            });
        }

        // reboot dead daemon process
        if ($restartDeadDaemon && $reboot) {
            foreach ($status as $sta) {
                if (!$sta['dead']) {
                    continue;
                }
                if ($this->rebootDeadWorker($sta)) {
                    usleep(100000);
                }
            }
            return $this->status();
        }
        return $status;
    }

    /**
     * @param $removePid
     * @return $this
     */
    protected function removeMasterPid($removePid)
    {
        $rebuild = false;
        $newPid = '';
        if (!is_array($removePid)) {
            $removePid = [$removePid];
        }
        $folder = $this->getPidFolderPrefixPath();
        $pidMaster = $this->getMasterHandler(function($buffer, $pid) use ($removePid, &$rebuild, &$newPid) {
            if (in_array($pid, $removePid)) {
                $rebuild = true;
            } else {
                $newPid .= $buffer;
            }
            return false;
        }, $folder.'pids');
        if (!$rebuild) {
            return $this;
        }
        $this->rebuildMasterFile(function() use ($pidMaster, $newPid, $folder, $removePid){
            ftruncate($pidMaster, 0);
            fseek($pidMaster, 0);
            fwrite($pidMaster, $newPid);
            foreach ($removePid as $pid) {
                @unlink($folder.$pid);
            }
        });
        return $this;
    }

    /**
     * @param null $file
     * @return string
     */
    protected function getPidFolderPrefixPath($file = null)
    {
        return $this->getPidFolder() . DIRECTORY_SEPARATOR . $this->getPidPrefix() . ($file ?: '');
    }

    /**
     * @param string $pidFile
     * @return array|bool|mixed
     */
    protected function getPidInfo($pidFile)
    {
        $content = file_get_contents($pidFile);
        $flags = @unpack('l4', substr($content, 0, 16));
        $info = @unserialize(substr($content, 16));
        if (!is_array($flags) || count($flags) != 4 || !is_array($info)) {
            throw new PidFileException('Pid file is error : '.$pidFile);
        }
        $info['daemon'] = $flags[2];
        $info['success'] = $flags[3];
        $info['failed'] = $flags[4];
        return $info;
    }

    /**
     * @param callable|null $loopCall
     * @param null $masterFile
     * @return resource
     */
    protected function getMasterHandler(callable $loopCall = null, $masterFile = null)
    {
        if (!$this->pidMaster) {
            if (!$masterFile) {
                $masterFile = $this->getPidFolderPrefixPath('pids');
            }
            $this->pidMaster = fopen($masterFile, 'a+b');
        }
        if ($loopCall) {
            fseek($this->pidMaster, 0, SEEK_SET);
            while ($buffer = fread($this->pidMaster, 4)) {
                $pid = unpack('l', $buffer);
                if (!is_array($pid) || !isset($pid[1])) {
                    throw new PidFileException('Pid master file is error : ' . $masterFile);
                }
                $pid = (int) $pid[1];
                if (call_user_func($loopCall, $buffer, $pid)) {
                    break;
                }
            }
        }
        return $this->pidMaster;
    }

    /**
     * @param callable $rebuild
     * @return $this
     */
    protected function rebuildMasterFile(callable $rebuild)
    {
        $lockFile = $this->getPidFolderPrefixPath('lock');
        while (true) {
            if (!is_file($lockFile)) {
                touch($lockFile);
                call_user_func($rebuild);
                unlink($lockFile);
                break;
            }
            usleep(10000);
        }
        return $this;
    }

    /**
     * @param $status
     * @return bool
     */
    protected function rebootDeadWorker($status)
    {
        if (!is_array($status) || !isset($status['php']) || !isset($status['startFile']) ||
            !isset($status['argv']) || !isset($status['cwd'])
        ) {
            return false;
        }
        return $this->tryToRun(function() use ($status) {
            $this->runDaemon($status['php'], $status['argv'], $status['cwd'], $status['startFile']);
            return true;
        });
    }

    /**
     * @param Closure $closure
     * @param bool $report
     * @return bool
     */
    protected function tryToRun(Closure $closure, $report = false)
    {
        try {
            $closure();
            return true;
        } catch (Exception $e) {
            if ($report) {
                $this->triggerException($e);
            }
        } catch (Throwable $e) {
            if ($report) {
                $this->triggerException($e);
            }
        }
        return false;
    }

    /**
     * @param $pid
     * @return bool
     */
    protected function isWorkerDead($pid)
    {
        $pid = (int) $pid;
        if (function_exists('posix_getpgid')) {
            return !(bool) posix_getpgid($pid);
        }
        if (DIRECTORY_SEPARATOR !== '\\') {
            $proc = false;
            if ($openDir = ini_get('open_basedir')) {
                $openDir = explode(PATH_SEPARATOR, $openDir);
                foreach ($openDir as $dir) {
                    if (strpos('/proc/', $dir) === 0) {
                        $proc = true;
                        break;
                    }
                }
            } else {
                $proc = true;
            }
            if ($proc) {
                return file_exists('/proc/'.$pid);
            }
        }
        if (!function_exists('shell_exec')) {
            throw new SystemException('function [shell_exec] not exists.');
        }
        $output = DIRECTORY_SEPARATOR === '\\' ? 'tasklist /fi "PID eq %d"' : 'ps -p %d';
        $output = shell_exec(sprintf($output, $pid));
        return !(strpos($output, (string) $pid) !== false && strpos($output, 'php') !== false);
    }

    /**
     * close file handler
     */
    public function __destruct()
    {
        if ($this->pidMaster) {
            @fclose($this->pidMaster);
        }
        if ($this->pidHandler) {
            @fclose($this->pidHandler);
        }
    }
}
