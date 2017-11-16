<?php
/*
Fetch data for RabbitMQ queue and store it in mongo database
*/
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPConnection;

function mongo_connect($db,$collection) {
	$m = new Mongo(); //handle username,ports and passwords
	$mydb = $m->$db;
	if(!isset($collection)) return $mydb;
	else {
		$myCollection = $mydb->$collection;
		return $myCollection;
	}
}

$connection = new AMQPConnection('localhost', 123, 'username', 'password');
$channel = $connection->channel();

$channel->queue_declare('testCollection', false, false, false, false);

echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";

$callback = function($msg) {
	$dataArray = json_decode($msg->body,true);
	$mongodb="testDatabase";
	$mongoCollection="testCollection";
	$collection= mongo_connect($mongodb,$mongoCollection);
	if($dataArray['action'] == 'page_load'){
		$collection->insert($dataArray);
	}else if($dataArray['action'] == 'page_unload'){
		$searchSessionId = $dataArray['session_id']; 
		$pageViewUpdate = $dataArray['page_view']; 
		
		$searchId = $collection->find(array('session_id'=>$searchSessionId,'page_view'=>$pageViewUpdate))-> sort(array('duration_start_datetime'=>-1))-> limit(1)-> getNext();
		$searchIdUser = $searchId['_id']->{'$id'};
		$durationStartDatetime = $searchId['duration_start_datetime'];
		
		$dateTime1 = new DateTime($durationStartDatetime);
		$dateTime2 = new DateTime($dataArray['duration_end_datetime']);
		$duration = $dateTime1->diff($dateTime2);
		$durationStayed = $duration->h.':'.$duration->i.':'.$duration->s;
		$dataArray['duration_stayed'] = $durationStayed;
		
		$newData = array('$set' => array("duration_end_datetime" => $dataArray['duration_end_datetime'],"duration_end_strtotime"=>$dataArray['duration_end_strtotime'],"duration_stayed"=>$dataArray['duration_stayed']));
		$collection->update(array('_id' => $searchId['_id']), $newData);
	}
};

$channel->basic_consume('testCollection', '', false, true, false, false, $callback);

while(count($channel->callbacks)) {
    $channel->wait();
}