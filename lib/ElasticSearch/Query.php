<?php

class ElasticSearch_Query
{

  const PUT    = 'PUT';
  const DELETE = 'DELETE';
  const GET    = 'GET';
  const POST   = 'POST';

  /** @var boolean */
  protected $_idRequired = FALSE;

  /** @var string */
  protected $_method;

  /** @var string */
  protected $_uri;

  /** @var string*/
  protected $_prefix;

  /** @var string|array */
  protected $_index;

  /** @var string|array */
  protected $_type;

  /** @var string */
  protected $_id;

  /** @var string */
  protected $_postfix;

  /** @var array */
  protected $_params = array();

  /** @var array */
  protected $_data = array();

  public function __construct($index = NULL)
  {
    $this->_index = $index;
  }

  public function index()
  {
    return $this->setMethod(self::PUT);
  }

  public function delete()
  {
    return $this->setMethod(self::DELETE)->idRequired(TRUE);
  }

  public function deleteByQuery()
  {
    return $this->setMethod(self::DELETE)->setId('_query');
  }

  public function get()
  {
    return $this->setMethod(self::GET);
  }

  public function search()
  {
    return $this->setMethod(self::GET)->setId('_search');
  }

  public function percolator()
  {
    return $this->setMethod(self::PUT)->setPrefix('_percolator');
  }

  public function percolate()
  {
    return $this->setMethod(self::GET)->setId('_percolate');
  }

  public function count()
  {
    return $this->setMethod(self::GET)->setId('_count');
  }

  public function moreLikeThis()
  {
    return $this->setMethod(self::GET)->setId('_mlt');
  }

  public function refresh()
  {
    return $this->setMethod(self::POST)->setId('_refresh');
  }

  public function idRequired($flag = NULL)
  {
    if($flag === NULL) {
      return $this->_idRequired;
    }
    $this->_idRequired = $flag;
    return $this;
  }

  public function getMethod()
  {
    if( ! $this->_method) {
      throw new Exception('No query method specified.');
    }
    return $this->_method;
  }

  public function setMethod($method)
  {
    $this->_method = $method;
    return $this;
  }

  public function getUri()
  {
    if($this->uri) {
      $uri = $this->_uri;
    }
    else {
      $uri = array();
      // Special cases where there is something before the index
      if($this->_prefix) {
        $uri[] = (string) $this->_prefix;
      }
      // Index always goes before type
      if($this->_index) {
        $uri[] = is_array($this->_index) ? implode(',',$this->_index) : (string) $this->_index;
      }
      else if($this->_type) {
        $uri[] = '_all';
      }
      // Type comes next
      if($this->_type) {
        $uri[] = is_array($this->_type) ? implode(',',$this->_type) : (string) $this->_type;
      }
      // Then id/action
      if($this->_id) {
        $uri[] = (string) $this->_id;
      } else if($this->_idRequired) {
        throw new Exception('The query requires an id.');
      }
      // Special cases where there is something after the id/action
      if($this->_postfix) {
        $uri[] = (string) $this->_postfix;
      }
      $uri = implode('/',$uri);
    }
    
    // Add parameters
    if($this->_params) {
      $params = array();
      foreach($this->_params as $key => $value) {
        $params[$key] = is_array($value) ? implode(',',$value) : (string) $value;
      }
      $uri .= '?'.http_build_query($params);
    }
    
    return $uri;
  }

  public function setUri($uri)
  {
    $this->_uri = $uri;
    return $this;
  }

  public function getPrefix()
  {
    return $this->_prefix;
  }

  public function setPrefix($prefix)
  {
    $this->_prefix = $prefix;
    return $this;
  }

  public function getIndex()
  {
    return $this->_index;
  }

  public function setIndex($index)
  {
    $this->_index = $index;
    return $this;
  }

  public function getType()
  {
    return $this->_type;
  }

  public function setType($type)
  {
    $this->_type = $type;
    return $this;
  }

  public function getId()
  {
    return $this->_id;
  }

  public function setId($id)
  {
    $this->_id = $id;
    return $this;
  }

  public function getPostfix()
  {
    return $this->_postfix;
  }

  public function setPostfix($postfix)
  {
    $this->_postfix = $postfix;
    return $this;
  }

  public function getParam($key = NULL)
  {
    if($key === NULL) {
      return $this->_params;
    } else {
      return isset($this->_params[$key]) ? $this->_params[$key] : NULL;
    }
  }

  public function setParam($key, $value = NULL)
  {
    if(is_array($key)) {
      $this->_params = $key;
    } else {
      $this->_params[$key] = $value;
    }
    return $this;
  }

  public function getData()
  {
    return $this->_data;
  }

  public function setData($key, $value = NULL)
  {
    if(is_array($key)) {
      $this->_data = $key;
    }
    else {
      $this->_data[$key] = $value;
    }
    return $this;
  }

  public function addData($key, $value = NULL, $_value = NULL)
  {
    if( ! is_array($key)) {
      if($_value === NULL) {
        return $this->addData(array($key => array($value => $_value)));
      }
      return $this->addData(array($key => $value));
    }

    $this->_data = self::array_merge($this->_data, $key);
    return $this;
  }

  public function inspect()
  {
    return $this->getMethod().':'.$this->getUri().'|'.json_encode($this->getData());
  }

  public function __toString()
  {
    return $this->inspect();
  }

  protected static function array_merge(array $array1, array $array2)
  {
    if( ! count($array1)) {
      return $array2;
    }

    foreach( $array2 as $key => $value )
    {
      if( is_array($value) && isset($array1[$key]) && count($value) && is_array($array1[$key]) )
      {
        $array1[$key] = self::array_merge ( $array1[$key], $value );
      }
      else
      {
        $array1[$key] = $value;
      }
    }
    return $array1;
  }
  
}
