<?php

namespace StatonLab\InterPro;

use Amp\MultiReasonException;
use Amp\Parallel\Worker\TaskError;
use function Amp\ParallelFunctions\parallel;
use function Amp\ParallelFunctions\parallelMap;
use Amp\Promise;
use StatonLab\TripalTestSuite\Services\BootstrapDrupal;
use StatonLab\TripalTestSuite\TripalTestBootstrap;

class InterproParallelImporter
{
    /**
     * The analysis id.
     *
     * @var int
     */
    protected $analysis_id;

    /**
     * Path to interpro scans directory that contains XMLs.
     *
     * @var string
     */
    protected $path;

    /**
     * Max number of jobs.
     *
     * @var int
     */
    protected $max_jobs;

    /**
     * The regexp to match the feature name.
     *
     * @var string
     */
    protected $regexp;

    /**
     * The query type.
     *
     * @var string
     */
    protected $type;

    /**
     * InterproParallelImporter constructor.
     *
     * @param int $analysis_id the analysis id to associate the annotations with.
     * @param string $path Path to IPR directory.
     * @throws \Exception
     */
    public function __construct($analysis_id, $path, $max_jobs = 5, $regexp = '(.*?)', $type = 'mRNA')
    {
        $this->analysis_id = $analysis_id;
        $this->path = $path;
        $this->max_jobs = $max_jobs;
        $this->regexp = $regexp;
        $this->type = $type;

        $this->validate();
    }

    /**
     * Validate the request.
     *
     * @throws \Exception
     */
    protected function validate()
    {
        if (empty($this->path) || ! $this->path) {
            throw new \Exception('Please provide a path to the xml files directory using --path=/path/to/dir');
        }

        if (empty($this->analysis_id) || ! $this->analysis_id) {
            throw new \Exception('Please provide an analysis id using --analysis_id=INTEGER.');
        }

        if (! file_exists($this->path)) {
            throw new \Exception($this->path.' does not exist or inaccessible. Please provide a valid path.');
        }

        if (! is_int($this->max_jobs)) {
            throw new \Exception('Please specify a valid max jobs number. Must be an integer.');
        }
    }

    /**
     * Start the importer.
     *
     * @throws \Exception
     */
    public function import()
    {
        // Generate the array
        $files = glob($this->getPath('*.xml'));

        if ($files === false) {
            throw new \Exception($this->path.' is inaccessible.');
        }

        $count = count($files);
        if ($count < 1) {
            throw new \Exception('Could not find any XML files in the specified path '.$this->path);
        }

        // clear out the anslysisfeature table for this analysis before getting started
        chado_delete_record('analysisfeature', ['analysis_id' => $this->analysis_id]);

        if ($count <= 10) {
            \Amp\call(function () {
                $this->parallelImport($this->path);

                return "Single job completed. Output printed to {$this->path}.out";
            })->onResolve(function ($error, $response) {
                if ($error) {
                    drush_print("Error: $error");

                    return;
                }

                drush_print($response);
            });
        }

        echo "Found $count files. Attempting to group files into $this->max_jobs batches.\n";
        $directories = $this->slice($files, $count);
        echo "Launching ".count($directories)." Jobs!\n";

        $values = Promise\wait(parallelMap($directories, function ($directory) {
            $start = time();
            $id = explode('_', $directory);
            $id = $id[count($id) - 1] + 1;
            $count = count(glob($directory.'/*.xml'));
            echo "Starting job #{$id} with $count files.\n";

            $this->parallelImport($directory);

            echo "Job #{$id} completed. Output printed to {$directory}.out\n";
            $end = time();

            return $end - $start;
        }));

        $total = array_reduce($values, function ($a, $carry) {
            return $a + $carry;
        });
        echo "Average time per job: ".($total / count($values))." seconds.";
    }

    /**
     * @param $files
     * @param $count
     * @return array
     * @throws \Exception
     */
    protected function slice($files, $count)
    {
        // per directory
        $per_directory = ceil($count / $this->max_jobs);
        $chunks = array_chunk($files, $per_directory);

        $directories = [];
        foreach ($chunks as $key => $chunk) {
            $path = $this->path.'/ips_batch_'.$key;
            if (! file_exists($path) && mkdir($path) === false) {
                throw new \Exception('Unable to create directory at '.$path.'. Please verify that you have write permissions.');
            }

            foreach ($chunk as $file) {
                $from = $file;
                $name = explode('/', $file);
                $to = $path.'/'.$name[count($name) - 1];
                if (copy($from, $to) === false) {
                    throw new \Exception('Unable to move file to new directory. From: '.$from.'. To: '.$to);
                }
            }

            $directories[] = $path;
        }

        return $directories;
    }

    /**
     * @param $directory
     * @throws \Exception
     */
    protected function parallelImport($directory)
    {
        $output = '';
        ob_start(function ($buffer) use (&$output) {
            $output .= $buffer;
        });
        $bootstrap = new BootstrapDrupal();
        $bootstrap->run();
        module_load_include('inc', 'tripal_analysis_interpro', 'includes/TripalImporter/InterProImporter');

        $importer = new \InterProImporter();

        $run_args = [
            'analysis_id' => $this->analysis_id,
            // optional
            'query_type' => $this->type,
            'query_re' => $this->regexp,
            'query_uniquename' => null,
            'parsego' => true,
        ];

        $importer->create($run_args, [
            'file_local' => $directory,
        ]);
        $importer->prepareFiles();
        $this->run($importer);

        $output .= ob_get_contents();
        file_put_contents($directory.'.out', $output);

        ob_end_clean();
    }

    /**
     * @param \InterProImporter $importer
     * @throws \ReflectionException
     */
    protected function run(\InterProImporter $importer)
    {
        $class = new \ReflectionClass(\InterProImporter::class);
        $property = $class->getProperty('arguments');
        $property->setAccessible(true);
        $arguments = $property->getValue($importer);
        $files = $arguments['files'];
        $arguments = $arguments['run_args'];
        $analysis_id = $arguments['analysis_id'];
        $interproxmlfile = trim($files[0]['file_path']);
        $parsego = $arguments['parsego'];
        $query_re = $arguments['query_re'];
        $query_type = $arguments['query_type'];
        $query_uniquename = $arguments['query_uniquename'];

        $this->parseXMLFile($importer, $analysis_id, $interproxmlfile, $parsego, $query_re, $query_type,
            $query_uniquename);
    }

    /**
     * @param \InterProImporter $importer
     * @param $analysis_id
     * @param $interproxmlfile
     * @param $parsego
     * @param $query_re
     * @param $query_type
     * @param $query_uniquename
     * @throws \Exception
     * @see \InterProImporter::parseXMLFile()
     */
    protected function parseXMLFile(
        \InterProImporter $importer,
        $analysis_id,
        $interproxmlfile,
        $parsego,
        $query_re,
        $query_type,
        $query_uniquename
    ) {
        //$transaction = db_transaction();
        try {
            // If user input a file(e.g. interpro.xml)
            if (is_file($interproxmlfile)) {
                $importer->parseSingleXMLFile($analysis_id, $interproxmlfile, $parsego, $query_re, $query_type,
                    $query_uniquename, 1);
            } else {
                // Parsing all files in the directory
                $dir_handle = @opendir($interproxmlfile);
                if (! $dir_handle) {
                    throw new \Exception('Unable to open dir '.$interproxmlfile);
                }
                $files_to_parse = [];
                while ($file = readdir($dir_handle)) {
                    if (preg_match("/^.*\.xml/i", $file)) {
                        $files_to_parse[] = $file;
                    }
                }

                $total_files = count($files_to_parse);
                $no_file = 0;

                foreach ($files_to_parse as $file) {
                    $importer->parseSingleXMLFile($analysis_id, "$interproxmlfile/$file", $parsego, $query_re,
                        $query_type, $query_uniquename, 0, $no_file, $total_files);
                }
            }
        } catch (\Exception $e) {
            //$transaction->rollback();
            echo $e->getMessage()."\n";
        }
    }

    /**
     * Gets the path to the main IPR directory.
     *
     * @param string $extension
     * @return string
     */
    protected function getPath($extension = '')
    {
        return $this->path.($extension ? "/$extension" : '');
    }
}
