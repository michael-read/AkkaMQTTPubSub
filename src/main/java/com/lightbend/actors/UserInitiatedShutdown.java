package com.lightbend.actors;

import akka.actor.CoordinatedShutdown;

public class UserInitiatedShutdown implements CoordinatedShutdown.Reason {
    @Override
    public String toString() {
        return this.getClass().getName();
    }
}