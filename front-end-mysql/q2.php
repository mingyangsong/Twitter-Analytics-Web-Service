<?php
    
    $TEAM_ID = 'MZW';
    $AWS_ACCOUNT_ID = '1101-8918-0417';
    
    if (isset($_GET['time'])) {
        $queryKey = $_GET['time'];
        
        MySQLConnection();
        
        $queryCmd = "select * from q2 where time = '$queryKey'";
        
        echo "$TEAM_ID, $AWS_ACCOUNT_ID\x0A";
        
        $result = mysql_query($queryCmd);
        while($row = mysql_fetch_row($result))
        {
            $data = str_replace("\\X0A", "\x0A", $row[1]);
            echo $data;
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