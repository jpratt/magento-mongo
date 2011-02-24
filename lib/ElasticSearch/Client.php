<?php

class ElasticSearch_Client
{

  /** @var ElasticSearch_Transport_Abstract */
  protected $_transport;

  /** @var string */
  protected $_index;

  public function __construct($transport, $index = NULL)
  {
    if($transport instanceof ElasticSearch_Transport_Abstract) {
      $this->_transport = $transport;
    } else if(is_array($transport)) {
      $className = isset($transport['transport']) ? $transport['transport'] : 'ElasticSearch_Transport_Socket';
      $this->_transport = new $className($transport);
    }

    if($index) {
      $this->_index = $index;
    }
  }

  public function getTransport()
  {
    return $this->_transport;
  }

  public function connect()
  {
    if( ! $this->getTransport()->isConnected()) {
      $this->getTransport()->connect();
    }
    return $this;
  }

  public function disconnect()
  {
    if($this->getTransport()->isConnected()) {
      $this->getTransport()->disconnect();
    }
    return $this;
  }

  public function getQuery()
  {
    return new ElasticSearch_Query($this->_index);
  }

  public function send(ElasticSearch_Query $query)
  {
    return $this->execute($query->getMethod(), $query->getUri(), $query->getData());
  }

  public function execute($method, $uri, $data = NULL)
  {
    $this->connect();
    return $this->getTransport()->$method($uri, is_string($data) ? $data : json_encode($data));
  }

}
