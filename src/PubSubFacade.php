<?php

namespace Averinuveren\LumenPubSub;

use Illuminate\Support\Facades\Facade;

/**
 * Class PubSubFacade
 * @package Superbalist\LaravelPubSub
 */
class PubSubFacade extends Facade
{
    /**
     * Get the registered name of the component.
     *
     * @return string
     */
    protected static function getFacadeAccessor()
    {
        return 'pubsub';
    }
}
