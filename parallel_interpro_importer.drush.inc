<?php
require_once __DIR__.'/vendor/autoload.php';
require_once __DIR__.'/include/InterproParallelImporter.php';

/**
 * Implements hook_drush_command().
 */
function parallel_interpro_importer_drush_command()
{
    return [
        'interpro-importer' => [
            'description' => 'Load InterProScan files in parallel.',
            'aliases' => ['interpro'],
            'options' => [
                'analysis_id' => '[int] the id of the analysis.',
                'path' => 'the path to the InterProScan files. This should point to a directory.',
                'max_jobs' => 'The maximum number of jobs to launch'
            ],
            'examples' => [
                'drush interpro --analysis_id=21 --path=/data/my_ipr_dir' => 'Starts loading IPR files located in /data/my_ipr_dir in parallel and associates them to analysis 21.',
            ],
        ],
    ];
}

/**
 * Implements drush_[MODULE_NAME]_[COMMAND_NAME]()
 */
function drush_parallel_interpro_importer_interpro_importer()
{
    $analysis_id = drush_get_option('analysis_id', null);
    $path = drush_get_option('path', null);
    $max_jobs = drush_get_option('max_jobs', 5);

    try {
        $importer = new \StatonLab\InterPro\InterproParallelImporter($analysis_id, $path, $max_jobs);
        $importer->import();
    } catch (Exception $e) {
        drush_print($e->getMessage());
    }
}
