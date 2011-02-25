<?php

class Cm_Mongo_Model_Resource_Type_ElasticSearch extends Mage_Core_Model_Resource_Type_Abstract
{

  /**
   * Get the ElasticSearch client
   *
   * @param Mage_Core_Model_Config_Element $config Connection config
   * @return ElasticSearch_Client
   */
  public function getConnection(Mage_Core_Model_Config_Element $config)
  {
    $client = new ElasticSearch_Client($config->asCanonicalArray(), (string)$config->index);

    // Set profiler
    //$client->set_profiler(array($this, 'start_profiler'), array($this, 'stop_profiler'));

    return $client;
  }

  public function start_profiler($group, $query)
  {
    $key = "$group::$query";
    Cm_Mongo_Profiler::start($key);
    return $key;
  }

  public function stop_profiler($key)
  {
    Cm_Mongo_Profiler::stop($key);
  }

}
