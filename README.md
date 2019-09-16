# Lumen-pubsub

A Pub-Sub abstraction for lumen.

The following adapters are supported:
* Local
* /dev/null
* Redis
* Kafka (see separate installation instructions below)
* Google Cloud
* HTTP

## Installation

```bash
composer require averinuveren2/lumen-pubsub
```

Register the service provider in app.php
```php
'providers' => [
    // ...
    Averinuveren\LumenPubSub\PubSubServiceProvider::class,
]
```

Register the facade in app.php
```php
'aliases' => [
    // ...
    'PubSub' => Averinuveren\LumenPubSub\PubSubFacade::class,
]
```

The package has a default configuration which uses the following environment variables.
```
PUBSUB_CONNECTION=redis

REDIS_HOST=localhost
REDIS_PASSWORD=null
REDIS_PORT=6379

KAFKA_BROKERS=localhost

GOOGLE_CLOUD_PROJECT_ID=your-project-id-here
GOOGLE_CLOUD_KEY_FILE=path/to/your/gcloud-key.json

HTTP_PUBSUB_URI=null
HTTP_PUBSUB_SUBSCRIBE_CONNECTION=redis
```

You can then edit the generated config at `app/config/pubsub.php`.

## Kafka Adapter Requirements

1. Install [librdkafka c library](https://github.com/edenhill/librdkafka)

    ```bash
    $ cd /tmp
    $ mkdir librdkafka
    $ cd librdkafka
    $ git clone https://github.com/edenhill/librdkafka.git .
    $ ./configure
    $ make
    $ make install
    ```
2. Install the [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka) PECL extension

    ```bash
    $ pecl install rdkafka
    ```
    
3. Add the following to your php.ini file to enable the php-rdkafka extension
    `extension=rdkafka.so`

## Usage

```php
// get the pub-sub manager
$pubsub = app('pubsub');

// note: function calls on the manager are proxied through to the default connection
// eg: you can do this on the manager OR a connection
$pubsub->publish('channel_name', 'message');

// get the default connection
$pubsub = app('pubsub.connection');
// or
$pubsub = app(\Superbalist\PubSub\PubSubAdapterInterface::class);

// get a specific connection
$pubsub = app('pubsub')->connection('redis');

// publish a message
// the message can be a string, array, bool, object - anything which can be json encoded
$pubsub->publish('channel_name', 'this is where your message goes');
$pubsub->publish('channel_name', ['key' => 'value']);
$pubsub->publish('channel_name', true);

// publish multiple messages
$messages = [
    'message 1',
    'message 2',
];
$pubsub->publishBatch('channel_name', $messages);

// subscribe to a channel
$pubsub->subscribe('channel_name', function ($message) {
    var_dump($message);
});

// all the above commands can also be done using the facade
PubSub::connection('kafka')->publish('channel_name', 'Hello World!');

PubSub::connection('kafka')->subscribe('channel_name', function ($message) {
    var_dump($message);
});
```

## Creating a Subscriber

The package includes a helper command `php artisan make:subscriber MyExampleSubscriber` to stub new subscriber command classes.

A lot of pub-sub adapters will contain blocking `subscribe()` calls, so these commands are best run as daemons running
as a [supervisor](http://supervisord.org) process.

This generator command will create the file `app/Console/Commands/MyExampleSubscriber.php` which will contain:
```php
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Superbalist\PubSub\PubSubAdapterInterface;

class MyExampleSubscriber extends Command
{
    /**
     * The name and signature of the subscriber command.
     *
     * @var string
     */
    protected $signature = 'subscriber:name';

    /**
     * The subscriber description.
     *
     * @var string
     */
    protected $description = 'PubSub subscriber for ________';

    /**
     * @var PubSubAdapterInterface
     */
    protected $pubsub;

    /**
     * Create a new command instance.
     *
     * @param PubSubAdapterInterface $pubsub
     */
    public function __construct(PubSubAdapterInterface $pubsub)
    {
        parent::__construct();

        $this->pubsub = $pubsub;
    }

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->pubsub->subscribe('channel_name', function ($message) {

        });
    }
}
```

### Kafka Subscribers ###

For subscribers which use the `php-pubsub-kafka` adapter, you'll likely want to change the `consumer_group_id` per
subscriber.

To do this, you need to use the `PubSubConnectionFactory` to create a new connection per subscriber.  This is because
the `consumer_group_id` cannot be changed once a connection is created.

Here is an example of how you can do this:

```php
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Averinuveren\LumenPubSub\PubSubConnectionFactory;
use Superbalist\PubSub\PubSubAdapterInterface;

class MyExampleKafkaSubscriber extends Command
{
    /**
     * The name and signature of the subscriber command.
     *
     * @var string
     */
    protected $signature = 'subscriber:name';

    /**
     * The subscriber description.
     *
     * @var string
     */
    protected $description = 'PubSub subscriber for ________';

    /**
     * @var PubSubAdapterInterface
     */
    protected $pubsub;

    /**
     * Create a new command instance.
     *
     * @param PubSubConnectionFactory $factory
     */
    public function __construct(PubSubConnectionFactory $factory)
    {
        parent::__construct();

        $config = config('pubsub.connections.kafka');
        $config['consumer_group_id'] = self::class;
        $this->pubsub = $factory->make('kafka', $config);
    }

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->pubsub->subscribe('channel_name', function ($message) {

        });
    }
}
```

## Adding a Custom Driver

Please see the [php-pubsub](https://github.com/Superbalist/php-pubsub) documentation  **Writing an Adapter**.

To include your custom driver, you can call the `extend()` function.

```php
$manager = app('pubsub');
$manager->extend('custom_connection_name', function ($config) {
    // your callable must return an instance of the PubSubAdapterInterface
    return new MyCustomPubSubDriver($config);
});

// get an instance of your custom connection
$pubsub = $manager->connection('custom_connection_name');
```
