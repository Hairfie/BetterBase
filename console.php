<?php

require_once __DIR__.'/vendor/autoload.php';

$httpAdapter = new Geocoder\HttpAdapter\BuzzHttpAdapter;

$providers = [
    'yandex'        => new Geocoder\Provider\YandexProvider($httpAdapter),
    'tomTom'        => new Geocoder\Provider\TomTomProvider($httpAdapter, 'wxu2jtphawhmqpmqbhb999ur'),
    'openStreetMap' => new Geocoder\Provider\OpenStreetMapProvider($httpAdapter),
    'googleMaps'    => new Geocoder\Provider\GoogleMapsProvider($httpAdapter),
];


$mongo = new MongoClient('dev.hairfie.com');
$db = $mongo->selectDB('hairfie-production');

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
                $progress->clear();
                $output->writeln($e->getMessage());
                $progress->display();
                $db->businesses->update(['_id' => $business['_id']], [
                    '$set' => [
                        $dataKey => ['error' => $e->getMessage()],
                    ]
                ]);
            }

            $progress->advance();

            usleep(100000); // 1/10s
        }
        $progress->finish();
        $output->writeln('');
    })
;

$app
    ->register('use-geo')
    ->setCode(function ($input, $output) use ($db) {
        $cursor = $db->businesses->find([
            'googleMapsGeo.streetNumber' => ['$exists' => true],
            'address.streetNumber'       => ['$exists' => false],
        ]);

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, $cursor->count());
        $progress->setFormat('debug');
        $progress->start();

        foreach ($cursor as $business) {
            $geo = array_merge(
                [
                    'latitude'     => null,
                    'longitude'    => null,
                    'streetNumber' => null,
                    'streetName'   => null,
                    'zipcode'      => null,
                    'city'         => null,
                    'countryCode'  => null,
                ],
                $business['googleMapsGeo']
            );

            $distanceMeters = haversineGreatCircleDistance(
                $business['gps']['lat'],
                $business['gps']['lng'],
                $geo['latitude'],
                $geo['longitude']
            );

            if ($distanceMeters > 750) {
                $progress->advance();
                continue;
            }

            $set = [];
            $set['originalGps'] = $business['gps'];
            $set['gps'] = ['lng' => $geo['longitude'], 'lat' => $geo['latitude']];
            $set['originalAddress'] = $business['address'];
            $set['address'] = [
                'streetNumber' => $geo['streetNumber'],
                'streetName'   => $geo['streetName'],
                'street'       => trim(sprintf('%s %s', $geo['streetNumber'], $geo['streetName'])) ?: $businesses['address']['street'],
                'city'         => $geo['city'] ?: $business['address']['city'],
                'zipCode'      => $geo['zipcode'] ?: $business['address']['zipCode'],
                'country'      => $geo['countryCode'] ?: $business['address']['country'],
            ];

            $db->businesses->update(['_id' => $business['_id']], ['$set' => $set]);

            $progress->advance();
        }

        $progress->finish();
        $output->writeln('');
    })
;

$app
    ->register('find-duplicates')
    ->setCode(function ($input, $output) use ($db) {
        $duplicates = [];

        if (!file_exists('index.php.cache')) {
            $businesses = $db->businesses->find([], [
                'name'        => true,
                'phoneNumber' => true,
                'address'     => true,
                'gps'         => true,
                'siret'       => true,
            ]);

            $output->writeln('Building index');

            $index = [
                'name'        => [],
                'phoneNumber' => [],
                'address'     => [],
                'siret'       => [],
                'businesses'  => [],
            ];

            $progress = new Symfony\Component\Console\Helper\ProgressBar($output, $businesses->count());
            $progress->setFormat('debug');
            $progress->start();
            $progress->display();

            foreach ($businesses as $business) {
                $id = (string)$business['_id'];
                $name = canonical($business['name']);
                $address = canonical(addressString($business['address']));
                $siret = empty($business['siret']) ? null : $business['siret'];
                $phoneNumber = empty($business['phoneNumber']) ? null : $business['phoneNumber'];

                $index['name'][$name][] = $id;
                $index['address'][$address][] = $id;
                if (null !== $siret) {
                    $index['siret'][$siret][] = $id;
                }
                if (null !== $phoneNumber) {
                    $index['phoneNumber'][$phoneNumber][] = $id;
                }

                $index['businesses'][$id] = [
                    'latitude'  => $business['gps']['lat'],
                    'longitude' => $business['gps']['lng'],
                ];

                $progress->advance();
            }

            $progress->finish();
            $output->writeln('');

            $output->writeln('Writing index cache');
            file_put_contents('index.php.cache', serialize($index));
        } else {
            $output->writeln('Reading index from cache');
            $index = unserialize(file_get_contents('index.php.cache'));
        }

        $output->writeln('Finding duplicates by siret');

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, count($index['siret']));
        $progress->setFormat('debug');
        $progress->start();
        $progress->display();

        foreach ($index['siret'] as $name => $ids) {
            if (1 === count($ids)) {
                $progress->advance();
                continue;
            }

            foreach ($ids as $referenceId) {
                foreach ($ids as $candidateId) {
                    if ($referenceId === $candidateId || $referenceId > $candidateId) {
                        continue;
                    }

                    $duplicates[$referenceId.' '.$candidateId][] = 'same siret';
                }
            }

            $progress->advance();
        }

        $progress->finish();
        $output->writeln('');

        $output->writeln('Finding duplicates by phone number');

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, count($index['phoneNumber']));
        $progress->setFormat('debug');
        $progress->start();
        $progress->display();

        foreach ($index['phoneNumber'] as $ids) {
            if (1 === count($ids)) {
                $progress->advance();
                continue;
            }

            foreach ($ids as $referenceId) {
                foreach ($ids as $candidateId) {
                    if ($referenceId === $candidateId || $referenceId > $candidateId) {
                        continue;
                    }

                    $duplicates[$referenceId.' '.$candidateId][] = 'same phone number';
                }
            }

            $progress->advance();
        }

        $progress->finish();
        $output->writeln('');

        $output->writeln('Finding duplicates by name');

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, count($index['name']));
        $progress->setFormat('debug');
        $progress->start();
        $progress->display();

        foreach ($index['name'] as $name => $ids) {
            if (1 === count($ids)) {
                $progress->advance();
                continue;
            }

            foreach ($ids as $referenceId) {
                $reference = $index['businesses'][$referenceId];
                foreach ($ids as $candidateId) {
                    if ($referenceId === $candidateId || $referenceId > $candidateId) {
                        continue;
                    }
                    $candidate = $index['businesses'][$candidateId];

                    $distance = haversineGreatCircleDistance(
                        $reference['latitude'],
                        $reference['longitude'],
                        $candidate['latitude'],
                        $candidate['longitude']
                    );

                    if ($distance < 1000) {
                        $duplicates[$referenceId.' '.$candidateId][] = sprintf('same name (distance: %sm)', round($distance));
                    }
                }
            }

            $progress->advance();
        }

        $progress->finish();
        $output->writeln('');

        $output->writeln('Finding duplicates by address');

        $progress = new Symfony\Component\Console\Helper\ProgressBar($output, count($index['address']));
        $progress->setFormat('debug');
        $progress->start();
        $progress->display();

        foreach ($index['address'] as $name => $ids) {
            if (1 === count($ids)) {
                $progress->advance();
                continue;
            }

            foreach ($ids as $referenceId) {
                foreach ($ids as $candidateId) {
                    if ($referenceId === $candidateId || $referenceId > $candidateId) {
                        continue;
                    }

                    $duplicates[$referenceId.' '.$candidateId][] = 'same address';
                }
            }

            $progress->advance();
        }

        $progress->finish();
        $output->writeln('');

        $output->writeln(sprintf('Found <info>%s</info> duplicate(s):', count($duplicates)));

        $json = [];
        $table = new Symfony\Component\Console\Helper\Table($output);
        $table->setHeaders(['Reference', 'Candidate', 'Reason']);

        foreach ($duplicates as $id => $reasons) {
            list($referenceId, $candidateId) = explode(' ', $id, 2);

            $table->addRow([$referenceId, $candidateId, implode($reasons, ' + ')]);
            $json[] = [
                'referenceId' => $referenceId,
                'candidateId' => $candidateId,
                'reasons'     => $reasons,
            ];
        }

        $table->render();
        $output->writeln(sprintf('Found <info>%s</info> duplicate(s):', count($duplicates)));

        file_put_contents('duplicates.json', json_encode($json, JSON_PRETTY_PRINT));
    })
;

function canonical($string)
{
    $string = strtolower(iconv('UTF-8', 'ASCII//TRANSLIT', $string));
    $string = preg_replace('/[^a-z0-9]/', '', $string);

    return $string;
}

function addressString($address)
{
    $address = array_merge(
        ['street' => null, 'city' => null, 'zipCode' => null, 'country' => null],
        (array)$address
    );

    return $address['street'].$address['city'].$address['zipCode'].$address['country'];
}

function notNull($value) { return null !== $value; };

function haversineGreatCircleDistance($latitudeFrom, $longitudeFrom, $latitudeTo, $longitudeTo)
{
    $earthRadius = 6371000;

    // convert from degrees to radians
    $latFrom = deg2rad($latitudeFrom);
    $lonFrom = deg2rad($longitudeFrom);

    $latTo = deg2rad($latitudeTo);
    $lonTo = deg2rad($longitudeTo);

    $latDelta = $latTo - $latFrom;
    $lonDelta = $lonTo - $lonFrom;

    $angle = 2 * asin(sqrt(pow(sin($latDelta / 2), 2) + cos($latFrom) * cos($latTo) * pow(sin($lonDelta / 2), 2)));

    return $angle * $earthRadius;
}

$app->run();
