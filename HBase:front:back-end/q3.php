<?php
    
    $TEAM_ID = 'MZW';
    $AWS_ACCOUNT_ID = '1101-8918-0417';
    
    if (isset($_GET['userid_min']) && isset($_GET['userid_max'])) {
        $queryMin = $_GET['userid_min'];
        $queryMax = $_GET['userid_max'];
        
        $queryMin = substr('0000000000000000000'. $queryMin, -19);
        $queryMax = substr('0000000000000000000'. $queryMax, -19);
        
        $dnsURL = '54.205.70.248';
        $dnsPort = '1339';
        
        $cmd = 'java clientTest '. $dnsURL. ' '. $dnsPort. ' '. $queryMin. ' '. $queryMax;
        
        $data = exec($cmd);
        
        echo "$TEAM_ID, $AWS_ACCOUNT_ID\x0A";
        echo $data;
        echo "\x0A";
    }
    else{
        echo "Not valid request varible";
    }
    
?>