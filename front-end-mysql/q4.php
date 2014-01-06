<?php
    
    $team="HMZ,1434-6455-2661";
    
    if (isset($_GET['userid'])) {
        $queryKey = $_GET['userid'];
        
        //$queryKey = substr('0000000000000000000'. $queryKey, -19);
        
        MySQLConnection();
        
        $queryCmd = "select * from q4 where user_from b = $queryKey";
        
        echo "$team\x0A";
        
        $result = mysql_query($queryCmd);
        while($row = mysql_fetch_row($result))
        {
            echo $row[1];
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