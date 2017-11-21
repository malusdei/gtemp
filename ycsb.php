<?php
namespace Google\Cloud\Samples\Spanner;
use Google\Cloud\Spanner\SpannerClient;
/*
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputDefinition;
*/
# Includes the autoloader for libraries installed with composer
require __DIR__ . '/vendor/autoload.php';
/*
Uasge:
php ycsb.php --instance=ycsb-bb9e6936 --database=ycsb table=usertable [--key=user1100197033673136279] --opcount=1 --perform=[LoadKeys|PerformRead|Update]
*/

$msg = "";

// Was going to try to multi thread, but Thread class is considered very dangerous in a CLI
// environment.  To multi-thread, please incorporate class into a PHP web page and make multiple
// calls to the same page.
//class SpannerOps extends Thread {
class SpannerOps {    
    // It is assumed that all calls are going out the same GRPC connection.
    // Please clarify if each thread should spawn its own GRPC.
    //public $instance;
    //public $database;
    //public $options;
    //public $testfunc;
    //global $msg;
    public function __construct() {
        //$this->$testfunc = $test;
        }

    public function LoadKeys($database, $options) {
        global $KEYS;
        $KEYS = array();
	$time_start = microtime(true);
        $snapshot = $database->snapshot();
        // Kind of assuming that id is ubiquitous...
        $results = $snapshot->execute('SELECT id FROM ' . $options['table']);
         foreach ($results as $row) {
            $KEYS[] = $row['id'];
            }
	return microtime() - $time_start;
        }

    public function PerformRead($database, $table, $key) {
        //Changed named to PerformRead because Read is a reserved keyword.
        global $KEYS;
        $KEYS = array();
	$time_start = microtime(true);
        $snapshot = $database->snapshot();
        // Kind of assuming that id is ubiquitous...
        $results = $snapshot->execute("SELECT * FROM $table where id = '$key'");
        /*
        foreach ($results as $row) {
            // Not sure why the original Python script does this.
            // We don't really need to parse results.
            $key = $row[0];
            }
	*/
	return microtime() - $time_start;
        }

    public function Update($database, $table, $key) {
        // Does a single update operation.
        $field = rand(1,10);
        // Generate a random value
        $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $charactersLength = strlen($characters);
        $randomValue = '';
	$time_start = microtime(true);
        for ($i = 0; $i < strlen($characters); $i++)
            $randomValue .= $characters[rand(0, $charactersLength - 1)];
        $operation = $database->transaction(['singleUse' => true])
            ->updateBatch($table, [
                ['id' => $key, "field".$field => $randomValue],
                ])
            ->commit();
	return microtime() - $time_start;
        }
	
    /* If we were going to use Threads, Threads would require us to have run() function
    public function run() {
        }
    */
    }


//Lives outside the class because it will only potentially be called once.
function parseCliOptions() {
    $longopts = array(
        "recordcount::",
        "opcount:",
        "clienttype::",
        "numworker::",
        "instance:",
        "database:",
        "table:",
        "perform:",
        "noskip_spanner_setup::",
	"skip_spanner_teardown::",
        "key::",
        );
    $options = getopt("", $longopts);
    var_dump($options);
    // Now we have things like $options["num_worker"]
    return $options;
    }

function OpenDatabase($options) {
    //global $database;
    $spanner = new SpannerClient();
    $instance = $spanner->instance($options['instance']);
    $database = $instance->database($options['database']);
    return $database;
    }

function ReportSwitch($strMsg) {
    global $msg;
    if (php_sapi_name() == 'cli') {
        print $strMsg;
        }
    else {
        // Otherwise, if it is being called from a browser, aggregate into a message.
        $msg .= $strMsg;
        }
}

// Allow for calling from a webserver
if (php_sapi_name() == 'cli') {
    $options = parseCliOptions();
    reportSwitch("Called from command line.\n");
    }
else {
    $options = parseQueryStringOptions();
    reportSwitch("Called from web browser.\n");
    }

foreach ($options as $opKey => $opVal) {
    reportSwitch("$opKey value is $opVal.\n");
    }

$testOp = new SpannerOps();

reportSwitch("Connecting to " . $options['database'] . "\n");

// Initial connection
$time_start = microtime(true);
$database = OpenDatabase($options);
$time_exec = microtime(true) - $time_start;
reportSwitch("Connected to " . $options['database'] . " in $time_exec seconds.\n");


for ($cntYCSB = 0; $cntYCSB < $options['opcount']; $cntYCSB++) {
    switch ($options['perform']) {
        case "LoadKeys":
            reportSwitch("Loaded keys in ".$testOp->LoadKeys($database, $options)." seconds. \n");
            break;
        case "PerformRead":
            reportSwitch("Performed Read in ".$testOp->PerformRead($database, $options['table'],"user1100197033673136279")." seconds.\n");
            break;
        case "Update":
            reportSwitch("Updated Key Val in ".$testOp->Update($database, $options['table'],"user1100197033673136279")." seconds.\n");
            break;
        default:
            break;
        }
    }

if ($msg !="") print $msg;

?>
