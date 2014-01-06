<?php
    
    $TEAM_ID = 'MZW';
    $AWS_ACCOUNT_ID = '1101-8918-0417';
    
    if (isset($_GET['userid_min']) && isset($_GET['userid_max'])) {
        $queryMin = $_GET['userid_min'];
        $queryMax = $_GET['userid_max'];
        
        //$queryMin = substr('0000000000000000000'. $queryMin, -19);
        //$queryMax = substr('0000000000000000000'. $queryMax, -19);
        
        MySQLConnection();
        
        $queryCmd = "select sum(count) from q3 where userid between $queryMin and $queryMax";
        
        echo "$TEAM_ID, $AWS_ACCOUNT_ID\x0A";
        
        $result = mysql_query($queryCmd);
        while($row = mysql_fetch_row($result))
        {
            echo $row[0];
            echo "\x0A";
        }
    }
    else{
        echo "Not valid request varible";
    }
    
    function MySQLConnection()
    {
        $dbuser = "root";
        $dbpass = "db15619Projects";
        $dbhost = "localhost:3306";
        $db = "tweet";
        $mysql = mysql_pconnect($dbhost,$dbuser,$dbpass)or die("fail:" . mysql_error() );
        mysql_select_db($db,$mysql);
        //echo "begin return";
        return $mysql;
        //echo 'connect sucessfully';
    }
    
?>