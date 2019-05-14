<?php

namespace PHPDots\ElasticSearchTailor;
use Elasticsearch\ClientBuilder;

class Elastic
{
	public function __construct()
	{
		$hosts = config('elasticConfig.hosts');
		$this->client = ClientBuilder::create()->setHosts($hosts)->setRetries(2)->build();
	}

		public function createIndex($name='default', $shards=1, $replicas=1)
		{
			$index['index'] = $name;
			$index['body']['number_of_shards'] = $shards;
			$index['body']['number_of_replicas'] = $replicas;

			return $this->client->indices()->create($index);
		}

		public function putSettings($index='default', $mappings, $type)
		{
			$params['index'] = $index;
			$params['type'] = $type;
			$params['body'][$type]['properties'] = $mappings;
			$params['body'][$type]['_source'] = [
				'enabled' => true
			];

			return $this->client->indices()->putMapping($params);
		}

		public function deleteIndex($index)
		{
			$params['index'] = $index;

			return $this->client->indices()->delete($params);
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
			$respones = [];
			if (!empty($body))
			{
				for ($i=0; $i < count($body); $i++)
				{
					$params['body'][] = [
						'index' => [
							'_index' => $index,
							'_type' => $type,
						]
					];

					$params['body'][] = $body[$i];

					if ($i % 1000)
					{
						$respones = $this->client->bulk($params);
						$params = [];
					}
				}
			}
			else
			{
				throw new Exception("Empty array for Indexing", 1);
				die();
			}

			return $respones;
		}

		public function bulkIndex($index, $indexType, $start_range)
		{
			$end_range = $start_range + 1000000;
			$description_path = public_path().DIRECTORY_SEPARATOR."daily_updates".DIRECTORY_SEPARATOR;
			if ($indexType == 'full')
			{
				if ($index == 'uspto')
				{
					echo "right";
					$sql_for_uspto_partial = "SELECT case_file.id, case_file.id as did, case_file_header.mark_identification, case_file.serial_number, case_file.registration_number,  CAST(case_file_header.filing_date as SIGNED ) as filing_date, CAST(case_file_header.registration_date as SIGNED ) as registration_date,
					case_file_header.status_code, classification.primary_code , case_file.case_file_owners_id
					FROM case_file JOIN case_file_header ON case_file.case_file_header_id=case_file_header.id
					JOIN correspondent ON correspondent.id=case_file.correspondent_id
					JOIN classifications_and_classification_map ON case_file.classifications_id=classifications_and_classification_map.classifications_id
					JOIN classification ON classifications_and_classification_map.classification_id=classification.id";
					$i = $start_range;
					$last_count = 0;
					while (true)
					{
						$sql_for_uspto_partial_limit = $sql_for_uspto_partial." LIMIT $i, 1000";

						$posts = \DB::select(\DB::raw($sql_for_uspto_partial_limit));
						$count = count($posts);

						$owner_ids = [];
						foreach ($posts as $post) 
						{
							$owner_ids[] = $post->case_file_owners_id;
						}

						$party_name_sql = "SELECT DISTINCT case_file_owner.party_name, case_file_owners_and_case_file_owner_map.case_file_owners_id FROM case_file_owners_and_case_file_owner_map
						JOIN case_file_owner ON case_file_owner.id=case_file_owners_and_case_file_owner_map.case_file_owner_id
						WHERE case_file_owners_and_case_file_owner_map.case_file_owners_id IN (".implode(',', $owner_ids).")";

						$party_names = \DB::select(\DB::raw($party_name_sql));

						foreach ($party_names as $party_name) 
						{
							$party_names2[$party_name->case_file_owners_id] = $party_name->party_name;
						}

						foreach ($posts as $post) 
						{
							$post_ownames = $party_names2[$post->case_file_owners_id];
							$post->party_name = $post_ownames;
						}

						$posts = array_map(function ($value) {
						    return (array)$value;
						}, $posts);

						$result = $this->bulkDataIndex('uspto', 'case_file', $posts);

						print_r($result);

						if ($count < 1000) 
						{
							$last_count = $i + $count;
							if ($last_count >= $end_range) {
								echo $last_count."\r\n";	
								break;
							}
							echo $last_count."\r\n";
							break;
						}
						$i += 1000;
					}
					$fp = fopen($description_path.'us_report.json', 'w');
					fwrite($fp, json_encode( ['last_inserted_count' => $last_count] ) );
					fclose($fp);
				}
				elseif ($index == 'euipo')
				{
					$sql_for_euipo_partial = "SELECT current.id, current.id as did, UNIX_TIMESTAMP(current.fil_date) as fil_date, current.file_nr,current.mark_text, current.markdescr, current.markdescrtext,CAST(current.statusapplication AS SIGNED) as statusapplication, current.mark_type_id,notlt.niceclmain, current.owner_gr_id FROM current JOIN notlt ON current.not_lt_id=notlt.id";

					$j = $start_range;
					$last_count = 0;
					while (true) 
					{
						$sql_for_euipo_partial_limit = $sql_for_euipo_partial." LIMIT $j, 1000";
						$posts = \DB::connection("mysql2")->select( \DB::raw($sql_for_euipo_partial_limit) );
						$count = count($posts);

						$owner_ids = [];
						foreach ($posts as $post) 
						{
							$owner_ids[] = $post->owner_gr_id;
						}

						$party_name_sql = "SELECT current.owner_gr_id,owner.owname FROM current JOIN owner_ids ON current.owner_gr_id=owner.ownergr_id WHERE owner.ownergr_id IN (".implode(',', $owner_ids).")";
						$party_names = \DB::connection("mysql2")->select( \DB::raw($party_name_sql) );

						foreach ($party_names as $party_name) 
						{
							$party_names2[$party_name->owner_gr_id] = $party_name->owname;
						}

						foreach ($posts as $post)
						{
							$post_ownames = $party_names2[$post->owner_gr_id];
							$post->owname = $post_ownames;
						}

						$posts = array_map(function ($value) {
						    return (array)$value;
						}, $posts);

						$result = $this->bulkDataIndex('euipo', 'current', $posts);

						if ($count < 1000)
						{
							$last_count = $j + $count;
							break;
						}
						$j += 1000;
					}
					$fp = fopen($description_path.'eu_report.json', 'w');
					fwrite($fp, json_encode( ['last_inserted_count' => $last_count] ) );
					fclose($fp);
				}
			}
			elseif ($indexType == 'parrtial')
			{
				$us_count_str = file_get_contents($description_path."us_report.json");
				$eu_count_str = file_get_contents($description_path."eu_report.json");
				if ($index == 'uspto')
				{
					$us_last_count_json = json_decode($us_count_str, true);
					$us_last_count = $us_last_count_json['last_inserted_count'];

					$sql_for_uspto_partial = "SELECT case_file.id, case_file.id as did, case_file_header.mark_identification, case_file.serial_number, case_file.registration_number,  CAST(case_file_header.filing_date as SIGNED ) as filing_date, CAST(case_file_header.registration_date as SIGNED ) as registration_date,
					case_file_header.status_code, classification.primary_code , case_file.case_file_owners_id
					FROM case_file JOIN case_file_header ON case_file.case_file_header_id=case_file_header.id
					JOIN correspondent ON correspondent.id=case_file.correspondent_id
					JOIN classifications_and_classification_map ON case_file.classifications_id=classifications_and_classification_map.classifications_id
					JOIN classification ON classifications_and_classification_map.classification_id=classification.id";
					$i = $us_last_count;
					$last_count = 0;
					while (true)
					{
						$sql_for_uspto_partial_limit = $sql_for_uspto_partial." LIMIT $i, 1000";
						$posts = \DB::select(\DB::raw($sql_for_uspto_partial_limit));
						$count = count($posts);

						$owner_ids = [];
						foreach ($posts as $post) 
						{
							$owner_ids[] = $post->case_file_owners_id;
						}

						$party_name_sql = "SELECT DISTINCT case_file_owner.party_name, case_file_owners_and_case_file_owner_map.case_file_owners_id FROM case_file_owners_and_case_file_owner_map
						JOIN case_file_owner ON case_file_owner.id=case_file_owners_and_case_file_owner_map.case_file_owner_id
						WHERE case_file_owners_and_case_file_owner_map.case_file_owners_id IN (".implode(',', $owner_ids).")";
						$party_names = \DB::select(\DB::raw($party_name_sql));

						foreach ($party_names as $party_name) 
						{
							$party_names2[$party_name->case_file_owners_id] = $party_name->party_name;
						}

						foreach ($posts as $post)
						{
							$post_ownames = $party_names2[$post->case_file_owners_id];
							$post->owname = $post_ownames;
						}

						$posts = array_map(function ($value) {
						    return (array)$value;
						}, $posts);

						$result = $this->bulkDataIndex('uspto', 'case_file', $posts);

						print_r($result);

						if ($count < 1000) 
						{
							$last_count = $i + $count;
							break;
						}
						$i += 1000;
					}
					$fp = fopen($description_path.'us_report.json', 'w');
					fwrite($fp, json_encode( ['last_inserted_count' => $last_count] ) );
					fclose($fp);
					
				}
				elseif ($index == 'euipo')
				{
					$eu_last_count_json = json_decode($eu_count_str, true);
					$eu_last_count = $eu_last_count_json['last_inserted_count'];

					$sql_for_euipo_partial = "SELECT current.id, current.id as did, UNIX_TIMESTAMP(current.fil_date) as fil_date, current.file_nr,current.mark_text, current.markdescr, current.markdescrtext,CAST(current.statusapplication AS SIGNED) as statusapplication, current.mark_type_id,notlt.niceclmain, current.owner_gr_id FROM current JOIN notlt ON current.not_lt_id=notlt.id";

					$j = 0;
					$last_count = 0;
					while (true) 
					{
						$sql_for_euipo_partial_limit = $sql_for_euipo_partial." LIMIT $j, 1000";
						$posts = \DB::connection("mysql2")->select( \DB::raw($sql_for_euipo_partial_limit) );
						$count = count($posts);

						$owner_ids = [];
						foreach ($posts as $post)
						{
							$owner_ids[] = $post->owner_gr_id;
						}

						$party_name_sql = "SELECT * FROM current JOIN owner_ids ON current.owner_gr_id=owner.ownergr_id WHERE owner.ownergr_id IN (".implode(',', $owner_ids).")";
						$party_names = \DB::connection("mysql2")->select( \DB::raw($party_name_sql) );

						foreach ($party_names as $party_name) 
						{
							$party_names2[$party_name->owner_gr_id] = $party_name->owname;
						}

						foreach ($posts as $post)
						{
							$post_ownames = $party_names2[$post->owner_gr_id];
							$post->owname = $post_ownames;
						}

						$posts = array_map(function ($value) {
						    return (array)$value;
						}, $posts);

						$result = $this->bulkDataIndex('euipo', 'current', $posts);

						print_r($result);

						if ($count < 1000)
						{
							$last_count = $j + $count;
							break;
						}
						$j += 1000;
					}
					$fp = fopen($description_path.'eu_report.json', 'w');
					fwrite($fp, json_encode( ['last_inserted_count' => $last_count] ) );
					fclose($fp);						
				}
			}

		}

		public function search($keyword, $field, $index, $type, $start_date, $last_date, $exact=0)
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
					$query['body']['query']['bool']['must'][]['query_string'] = [
						'query' => "\"$keyword\"",
						'fields' => implode(',', $field)
					];
				}
				elseif (is_string($field))
				{
					$query['body']['query']['bool']['must'][]['query_string'] = [
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
					$query['body']['query']['bool']['must'][]['multi_match'] = [
						'query' => $keyword,
						'fields' => implode(',', $field)
					];
				}
				elseif (is_string($field))
				{
					$query['body']['query']['bool']['must'][]['match'] = [
						$field => $keyword
					];
				}
				else
				{
					throw new Exception("Invalid Field for Search", 1);
					exit;
				}
			}

			if ($index == 'uspto')
			{
				$query['body']['query']['bool']['must'][]['range']['filing_date'] = [
					'gte' => $start_date,
					'lte' => $last_date
				];
			}
			elseif ($index == 'euipo')
			{
				$query['body']['query']['bool']['must'][]['range']['fil_date'] = [
					'gte' => $start_date,
					'lte' => $last_date
				];
			}

			$result = $this->client->search($query);

			return $result['hits']['hits'];
		}

	}

	?>
