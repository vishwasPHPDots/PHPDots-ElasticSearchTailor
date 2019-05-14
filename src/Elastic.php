<?php

namespace PHPDots\ElasticSearchTailored;

class Elastic
{
	public function __construct()
	{
		$hosts = config('elasticConfig.host');
		$this->client = ClientBuilder::create()->setHosts($hosts)->setRetries(2)->build();
	}

	public function createIndex($name='default', $shards=0, $replicas=0, $mappings=[])
	{
		$index['index'] = $name;
		$index['body']['setting']['number_of_shards'] = $shards;
		$index['body']['setting']['number_of_replicas'] = $replicas;

		if (!empty($mappings))
		{
			$index['body']['mappings']['my_type'] = $mappings;
		}

		$this->client->indices()->create($index);
	}

	public function putSettings($name='default', $mappings, $type)
	{
		$params['index'] = $name;
		$params['type'] = $type;
		$params['body'][$type] = $mappings;

		$this->client->indices()->putMapping($params);
	}

	public function deleteIndex($name)
	{
		$params['index'] = $name;

		$this->client->indices()->delete($params);
	}

	public function singleDataIndex($index, $type, $body)
	{
		$params['body'] = $body;
		$params['type'] = $type;
		$params['index'] = $index;

		if (empty($body))
		{
			throw new Exception("Empty array for indexing", 1);
			exit();
		}

		$ret = $this->client->index($params);
		return $ret;
	}

	public function bulkDataIndex($index, $type, $body)
	{
		if (!empty($body))
		{
			for ($i=0; $i < count($body); $i++)
			{
				$params['body'][] = [
					'index' => [
						'index' => $index,
						'type' => $type,
						'body' => $body[$i]
					],
				];

				if ($i % 1000)
				{
					$respones = $this->client->bulk($params);
					$params = [];
					unset($respones);
				}
			}
		}
		else
		{
			throw new Exception("Empty array for Indexing", 1);
			die();
		}
	}

	public function search($keyword, $field, $index, $type)
	{
		$query = [];
		$query['index'] = $index;
		if (!empty($type)) 
		{
			$query['type'] = $type;			
		}

		if ($exact)
		{
			if (is_array($field)) 
			{
				$query['body']['query']['bool']['should'][]['query_string'] = [
					'query' => "\"$keyword\"",
					'fields' => $field
				];
			}
			elseif (is_string($field)) 
			{
				$query['body']['query']['bool']['should'][]['query_string'] = [
					'default_field' => $field,
					'query' => $keyword
				];			
			}
			else
			{
				throw new Exception("Invalid Field for Search", 1);
				exit;
			}
		}
		else
		{
			if (is_array($field)) 
			{
				$query['body']['query']['bool']['should'][]['multi_match'] = [
					'query' => $keyword,
					'fields' => $field
				];
			}
			elseif (is_string($field)) 
			{
				$query['body']['query']['bool']['should'][]['match'] = [
					$field => $keyword
				];			
			}
			else
			{
				throw new Exception("Invalid Field for Search", 1);
				exit;
			}
		}

		$query['body']['query']['bool']['must'][]['range']['userid'] = [
			'gte' => $start_id,
			'lte' => $last_id
		];

		$result = $this->client->search($query);

		return $result['hits']['hits'];
	}

}

?>
