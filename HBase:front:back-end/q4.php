<?php
    
    $TEAM_ID = 'MZW';
    $AWS_ACCOUNT_ID = '1101-8918-0417';
    
    if (isset($_GET['userid'])) {
        $queryKey = $_GET['userid'];
        
        $queryKey = str_replace(' ', '%20', $queryKey);
        
        $dnsURL = 'ec2-54-205-70-248.compute-1.amazonaws.com';
        $dnsPort = '1339';
        
        $serverURL = 'http://'. $dnsURL. ':'. $dnsPort;
        
        $tableName = 'UserReUsers';
        $familyName = 'Users:all';
        
        $url = $serverURL. '/'. $tableName. '/'. $queryKey. '/'. $familyName. '/';
        
        fileGet_print($url, $TEAM_ID, $AWS_ACCOUNT_ID);
        
    } else {
        echo "Not valid request varible";
    }
    
    function fileGet_print($url, $TEAM_ID, $AWS_ACCOUNT_ID){
        
        if(($fileRes = file_get_contents($url)) != false){
            
            $data = str_replace('\x0A', '<br>', $fileRes);
            
            echo "$TEAM_ID, $AWS_ACCOUNT_ID\x0A";
            echo $data;
            echo "\x0A";
            
        } else{
            echo "HBase Server Connection Fail.";
        }
    }
    
?>