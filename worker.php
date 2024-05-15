<?php

$gmworker= new GearmanWorker();

$gmworker->addServer("192.168.64.1");
$gmworker->addFunction("reverse", "reverse");
$gmworker->addFunction("not_reverse", "not_reverse");
$gmworker->addFunction("not_reverse_bla", "not_reverse");


print "Waiting for job...\n";
while($gmworker->work()) {
  if ($gmworker->returnCode() != GEARMAN_SUCCESS) {
    echo "return_code: " . $gmworker->returnCode() . "\n";
    break;
  }
}

function not_reverse($job) {
  echo sprintf(" fn: %s payload: %s\n", $job->functionName(), $job->workload());
//   sleep(10);
  return $job->workload();
}


function reverse($job) {
  echo sprintf(" fn: %s payload: %s\n", $job->functionName(), $job->workload());
//   sleep(10);

  return strrev($job->workload());
}


?>