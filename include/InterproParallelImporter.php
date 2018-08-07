<?php

namespace StatonLab\InterPro;

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
    public function __construct($analysis_id, $path, $max_jobs = 5, $regexp = '/(.*?)/', $type = 'mRNA')
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
     * @return array[\Amp\Promise]
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

        module_load_include('inc', 'tripal_analysis_interpro', 'includes/TripalImporter/InterProImporter');

        // clear out the anslysisfeature table for this analysis before getting started
        chado_delete_record('analysisfeature', ['analysis_id' => $this->analysis_id]);

        $promises = [];

        if ($count <= 10) {
            $promises[] = \Amp\call(function () {
                $this->parallelImport($this->path);

                return "Single job completed. Output printed to {$this->path}.out";
            });

            return $promises;
        }

        $directories = $this->slice($files, $count);
        foreach ($directories as $directory) {
            $promises[] = \Amp\call(function () use ($directory) {
                $this->parallelImport($directory);

                return "{$directory} completed. Output printed to {$directory}.out";
            });
        }

        return $promises;
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
            $path = $this->path.'/ipr_batch_'.$key;
            if (mkdir($path) === false) {
                throw new \Exception('Unable to create directory at '.$path.'. Please verify that you have write permissions.');
            }

            foreach ($chunk as $file) {
                $from = $this->path.'/'.$file;
                $to = $path.'/'.$file;
                if (rename($from, $to) === false) {
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
        echo "Starting job $directory\n";
        $output = '';
        ob_start(function ($buffer) use (&$output) {
            $output .= $buffer;
        });

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
        $analysis_id = $arguments['analysis_id'];
        $interproxmlfile = trim($arguments['files'][0]['file_path']);
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
        $transaction = db_transaction();
        try {
            // If user input a file(e.g. interpro.xml)
            if (is_file($interproxmlfile)) {
                $importer->parseSingleXMLFile($analysis_id, $interproxmlfile, $parsego, $query_re, $query_type,
                    $query_uniquename, 1);
            } else {
                // Parsing all files in the directory
                $dir_handle = @opendir($interproxmlfile);
                if (! $dir_handle) {
                    throw new \Exception('Unable to open dir');
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
            $transaction->rollback();
            $importer->logMessage(t("\nFAILED: Rolling back database changes...\n"));
        }
        $importer->logMessage(t("\nDone\n"));
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
