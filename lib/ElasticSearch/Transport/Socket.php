<?php

class ElasticSearch_Transport_Socket extends ElectricSearch_Transport_Abstract
{

  protected $_config = array(
      'host'       => 'localhost',
      'port'       => '9200',
      'persistent' => false,
      'timeout'    => 30,
  );

  protected $_socket;

  public function connect()
  {
    if( ! $this->_connected) {
      $flags = STREAM_CLIENT_CONNECT;
      if ($this->_config['persistent']) $flags |= STREAM_CLIENT_PERSISTENT;

      $this->_socket = @stream_socket_client(
        'tcp://'.$this->_config['host'].':'.$this->_config['port'],
        $errno,
        $errstr,
        (int) $this->_config['timeout'],
        $flags,
        stream_context_create()
      );
      @stream_set_timeout($this->_socket, (int) $this->_config['timeout']);
      $this->_connected = TRUE;
    }
  }

  public function disconnect()
  {
    if($this->_connected) {
      fclose($this->_socket);
      $this->_socket = NULL;
      $this->_connected = FALSE;
    }
  }

  public function put($uri, $data)
  {
    return $this->_request('PUT', $uri, $data);
  }

  public function delete($uri, $data)
  {
    return $this->_request('DELETE', $uri, $data);
  }

  public function get($uri, $data)
  {
    return $this->_request('GET', $uri, $data);
  }

  public function post($uri, $data)
  {
    return $this->_request('POST', $uri, $data);
  }

  protected function _request($method, $uri, $data)
  {
    $this->_connected or $this->connect();
    
    $request =
    "$method /$uri HTTP/1.1\r\n".
    "Content-Length: ".strlen($data)."\r\n".
    //"Connection: close\r\n".  // Keep-Alive
    "\r\n";

    if( ! fwrite($this->_socket, $request)) {
      throw new Exception('Error sending request header.');
    }
    if($data && ! fwrite($this->_socket, $data)) {
      throw new Exception('Error sending request body.');
    }

    $line = @fgets($this->_socket);
    if($line === FALSE || ! preg_match("|^HTTP/[\d\.x]+ (\d+) ([^\r\n]+)|", $line, $matches)) {
      throw new Exception('Invalid response: '.$line);
    }

    if($matches[1] != '200') {
      throw new Exception('Error: '.$matches[1].' '.$matches[2]);
    }

    $headers = '';
    while(($line = @fgets($this->_socket)) !== FALSE) {
      $line = trim($line);
      if( ! $line) break;
      list($name, $value) = explode(':',$line,2);
      $headers[strtolower($name)] = trim($value);
    }

    if(isset($headers['content-length']) && ($length = (int) $headers['content-length'])) {
      $body = fread($this->_socket, $length);
    }
    else {
      $body = '';
      while( ! feof($this->_socket)) {
        $body .= @fread($this->_socket, 8192);
      }
    }

    if(isset($headers['connection']) && $headers['connection'] == 'close') {
      $this->disconnect();
    }

    return json_decode($body, TRUE);
  }

  public function __destruct()
  {
    if (! $this->config['persistent'] && $this->_socket) $this->close();
  }

}
