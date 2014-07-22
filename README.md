# Blueprint

The schema server for the
[Twitch Science Data Pipeline](https://github.com/TwitchScience).

## What it does

This permits you to create "schemas". A schema is essentially a
confluence of two related concepts:

 # A set of instructions for how to convert inbound data to the
   desired outbound data
 # The table structure that the data will be inserted into

"Outbound data" and "table structure" are essentially the same in our
world view.

## Components

 + An angularjs frontend
 + An API
 + [scoop](https://github.com/TwitchScience/scoop)

The frontend works with the API to create tables, scoop handles the
creation of those tables.

Systems that wish to know about the schemas currently route to scoop
by way of blueprint.

## Improvements

Improve these docs!
