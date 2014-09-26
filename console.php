<?php

require_once __DIR__.'/vendor/autoload.php';

$httpAdapter = new Geocoder\HttpAdapter\BuzzHttpAdapter;

$providers = [
    'yandex'        => new Geocoder\Provider\YandexProvider($httpAdapter),
    'tomTom'        => new Geocoder\Provider\TomTomProvider($httpAdapter, 'wxu2jtphawhmqpmqbhb999ur'),
    'openStreetMap' => new Geocoder\Provider\OpenStreetMapProvider($httpAdapter),
];


$mongo = new MongoClient('dev.hairfie.com');
$db = $mongo->selectDB('hairfie-staging');

$app = new Symfony\Component\Console\Application;
$app
    ->register('run')
    ->addArgument('provider', Symfony\Component\Console\Input\InputArgument::REQUIRED)
    ->addOption('override')
    ->setCode(function ($input, $output) use ($providers, $db) {
        $provider = $providers[$input->getArgument('provider')];
        $dataKey  = sprintf('%sGeo', $input->getArgument('provider'));
        $query    = ['address' => ['$exists' => true]];

        if (!$input->getOption('override')) {
            $query[$dataKey] = ['$exists' => false];
        }

        $cursor = $db->businesses->find($query);

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, $cursor->count());
        $progress->setFormat('debug');
        $progress->start();

        $geocoder = new Geocoder\Geocoder($provider);

        foreach ($cursor as $business) {
            $address = sprintf(
                '%s, %s %s, FRANCE',
                str_replace(' pl ', ' place ', $business['address']['street']),
                $business['address']['city'],
                $business['address']['zipCode']
            );

            try {
                $result = $geocoder->geocode($address);
                $db->businesses->update(['_id' => $business['_id']], [
                    '$set' => [
                        $dataKey => array_filter($result->toArray(), 'notNull'),
                    ]
                ]);
            } catch (Geocoder\Exception\NoResultException $e) {
                $output->writeln($e->getMessage());
            }

            $progress->advance();
        }
        $progress->finish();
        $output->writeln('');
    })
;

function notNull($value) { return null !== $value; };

$app->run();
