<?php
class PHPUNIT_LOADER
{
    public static $status = 0;

    /**
     * @var Composer\Autoload\ClassLoader|Tanbolt\Loader
     */
    public static $loader = null;

    /**
     * init auto loader
     */
    public static function init()
    {
        if (is_file($file = __DIR__ . '/../../loader.php')) {
            // tanbolt loader
            $loader = require $file;
            if (!$loader instanceof Tanbolt\Loader) {
                die('find tanbolt loader file, but class \\Tanbolt\\Loader not exist.');
            }
            $loader->test(true);
            static::$status = 1;
            static::$loader = $loader;
        } elseif (is_file($file = __DIR__.'/../../../autoload.php')) {
            // composer autoload
            $loader = require $file;
            if (!$loader instanceof Composer\Autoload\ClassLoader) {
                die('find composer autoload file, but class \\Composer\\Autoload\\ClassLoader not exist.');
            }
            static::$status = 2;
            static::$loader = $loader;
        } else {
            // die
            die('can\'t find composer autoload or tanbolt loader.');
        }
        if ('' === ini_get('date.timezone')) {
            ini_set('date.timezone','UTC');
        }
    }

    /**
     * add namespace group
     * @param $prefix
     * @param $dir
     * @return bool
     */
    public static function addDir($prefix, $dir)
    {
        if (static::$status > 1) {
            if ('\\' == $prefix[0]) {
                $prefix = substr($prefix, 1);
            }
            if ('\\' !== $prefix[strlen($prefix) - 1]) {
                $prefix = $prefix.'\\';
            }
            static::$loader->setPsr4($prefix, $dir);
            return true;
        }
        if (static::$status > 0) {
            static::$loader->setDir($prefix, $dir);
            return true;
        }
        return false;
    }

    /**
     * add namespace
     * @param $prefix
     * @param $file
     * @return bool
     */
    public static function addFile($prefix, $file)
    {
        if (static::$status > 1) {
            if ('\\' == $prefix[0]) {
                $prefix = substr($prefix, 1);
            }
            static::$loader->addClassMap([$prefix => $file]);
            return true;
        }
        if (static::$status > 0) {
            static::$loader->setFile($prefix, $file);
            return true;
        }
        return false;
    }
}
PHPUNIT_LOADER::init();
