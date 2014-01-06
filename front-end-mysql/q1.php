<?php
    
    $TEAM_ID = 'MZW';
    $AWS_ACCOUNT_ID = '1101-8918-0417';
    
    date_default_timezone_set('EST');
    $curTimeStamp = date('Y-m-d H:i:s');
    
    echo "$TEAM_ID, $AWS_ACCOUNT_ID\x0A";
    echo $curTimeStamp;
    echo "\x0A";
    
?>