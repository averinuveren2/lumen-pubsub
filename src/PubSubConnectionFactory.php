<?php

namespace Averinuveren\LumenPubSub;

use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Arr;
use InvalidArgumentException;
use Superbalist\PubSub\Adapters\DevNullPubSubAdapter;
use Superbalist\PubSub\Adapters\LocalPubSubAdapter;
use Superbalist\PubSub\GoogleCloud\GoogleCloudPubSubAdapter;
use Superbalist\PubSub\HTTP\HTTPPubSubAdapter;
use Superbalist\PubSub\Kafka\KafkaPubSubAdapter;
use Superbalist\PubSub\PubSubAdapterInterface;
use Superbalist\PubSub\Redis\RedisPubSubAdapter;

/**
 * Class PubSubConnectionFactory
 * @package Superbalist\LaravelPubSub
 */
class PubSubConnectionFactory
{
    /**
     * @var Container
     */
    protected $container;

    /**
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Factory a PubSubAdapterInterface.
     * @param $driver
     * @param array $config
     * @return DevNullPubSubAdapter|LocalPubSubAdapter|GoogleCloudPubSubAdapter|HTTPPubSubAdapter|KafkaPubSubAdapter|RedisPubSubAdapter
     * @throws BindingResolutionException
     */
    public function make($driver, array $config = []): PubSubAdapterInterface
    {
        switch ($driver) {
            case '/dev/null':
                return new DevNullPubSubAdapter();
            case 'local':
                return new LocalPubSubAdapter();
            case 'redis':
                return $this->makeRedisAdapter($config);
            case 'kafka':
                return $this->makeKafkaAdapter($config);
            case 'gcloud':
                return $this->makeGoogleCloudAdapter($config);
            case 'http':
                return $this->makeHTTPAdapter($config);
        }

        throw new InvalidArgumentException(sprintf('The driver [%s] is not supported.', $driver));
    }

    /**
     * Factory a RedisPubSubAdapter.
     * @param array $config
     * @return PubSubAdapterInterface
     * @throws BindingResolutionException
     */
    protected function makeRedisAdapter(array $config): PubSubAdapterInterface
    {
        if (!isset($config['read_write_timeout'])) {
            $config['read_write_timeout'] = 0;
        }

        $client = $this->container->make('pubsub.redis.redis_client', ['config' => $config]);

        return new RedisPubSubAdapter($client);
    }

    /**
     * Factory a KafkaPubSubAdapter.
     * @param array $config
     * @return KafkaPubSubAdapter
     * @throws BindingResolutionException
     */
    protected function makeKafkaAdapter(array $config): PubSubAdapterInterface
    {
        // create producer
        $producer = $this->container->make('pubsub.kafka.producer');
        $producer->addBrokers($config['brokers']);

        // create consumer
        $topicConf = $this->container->make('pubsub.kafka.topic_conf');
        $topicConf->set('auto.offset.reset', 'smallest');

        $conf = $this->container->make('pubsub.kafka.conf');
        $conf->set('group.id', Arr::get($config, 'consumer_group_id', 'php-pubsub'));
        $conf->set('metadata.broker.list', $config['brokers']);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('offset.store.method', 'broker');
        $conf->setDefaultTopicConf($topicConf);

        $consumer = $this->container->make('pubsub.kafka.consumer', ['conf' => $conf]);

        return new KafkaPubSubAdapter($producer, $consumer);
    }

    /**
     * Factory a GoogleCloudPubSubAdapter.
     * @param array $config
     * @return GoogleCloudPubSubAdapter
     * @throws BindingResolutionException
     */
    protected function makeGoogleCloudAdapter(array $config): PubSubAdapterInterface
    {
        $clientConfig = [
            'projectId' => $config['project_id'],
            'keyFilePath' => $config['key_file'],
        ];
        if (isset($config['auth_cache'])) {
            $clientConfig['authCache'] = $this->container->make($config['auth_cache']);
        }

        $client = $this->container->make('pubsub.gcloud.pub_sub_client', ['config' => $clientConfig]);

        $clientIdentifier = Arr::get($config, 'client_identifier');
        $autoCreateTopics = Arr::get($config, 'auto_create_topics', true);
        $autoCreateSubscriptions = Arr::get($config, 'auto_create_subscriptions', true);
        $backgroundBatching = Arr::get($config, 'background_batching', false);
        $backgroundDaemon = Arr::get($config, 'background_daemon', false);

        if ($backgroundDaemon) {
            putenv('IS_BATCH_DAEMON_RUNNING=true');
        }
        return new GoogleCloudPubSubAdapter(
            $client,
            $clientIdentifier,
            $autoCreateTopics,
            $autoCreateSubscriptions,
            $backgroundBatching
        );
    }

    /**
     * Factory a HTTPPubSubAdapter.
     * @param array $config
     * @return HTTPPubSubAdapter
     * @throws BindingResolutionException
     */
    protected function makeHTTPAdapter(array $config): PubSubAdapterInterface
    {
        $client = $this->container->make('pubsub.http.client');
        $adapter = $this->make(
            $config['subscribe_connection_config']['driver'],
            $config['subscribe_connection_config']
        );
        return new HTTPPubSubAdapter($client, $config['uri'], $adapter);
    }
}
