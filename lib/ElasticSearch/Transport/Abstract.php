<?php

abstract class ElasticSearch_Transport_Abstract
{

  const PUT    = 'PUT';
  const DELETE = 'DELETE';
  const GET    = 'GET';
  const POST   = 'POST';

  /** @var array */
  protected $_config = array();

  /** @var boolean */
  protected $_connected = FALSE;

  public function __construct($config)
  {
    foreach($config as $key => $value) {
      $this->_config[$key] = $value;
    }
    return $this;
  }

  abstract public function connect();

  abstract public function disconnect();

  public function isConnected()
  {
    return $this->_connected;
  }

  public function execute($method, $uri, $data)
  {
    switch($method)
    {
      case self::PUT:
        return $this->put($uri, $data);
      case self::DELETE:
        return $this->delete($uri, $data);
      case self::GET:
        return $this->get($uri, $data);
      case self::POST:
        return $this->post($uri, $data);
      default:
        throw new Exception('Invalid method: '.$method);
    }
  }

  abstract protected function put($uri, $data);

  abstract protected function delete($uri, $data);

  abstract protected function get($uri, $data);

  abstract protected function post($uri, $data);

}
