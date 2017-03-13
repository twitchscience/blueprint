# Blueprint

The schema server for the
[Twitch Science Data Pipeline](https://github.com/TwitchScience).

See also the [Schema Suggestor](./schema_suggestor/README.md).

## What it does

This permits you to create "schemas". A schema is essentially a
confluence of two related concepts:

 * A set of instructions for how to convert inbound data to the
   desired outbound data
 * The table structure that the data will be inserted into

"Outbound data" and "table structure" are essentially the same in our
world view.

## Components

 + An angularjs frontend
 + An API
 + A postgres db storing schema state

The frontend works with the API to create schemas in bpdb, the ingesters handle the
creation of those tables later.

## Building

```
apt-get -u install build-essential libgeoip-dev libgeoip1
go get github.com/tools/godep
./build.sh blueprint $GIT_BRANCH $BASEAMI $SECURITY_GROUP false
```

The arguments to `build.sh` are position, each position is as follows:

 1. `project`: The project name, in this case "blueprint"
 2. `branch`: The name of your branch
 3. `source_ami`: AMI to use as the base for resultant AMI
 4. `security_group`: Security Group ID to use
 5. `use_private_ip`: SSH to private IP (default: false)

Packer expects there to be two environment variables available:

 * `AWS_ACCESS_KEY`
 * `AWS_SECRET_KEY`

Setup your security group to permit access from your IP address.

Set the `use_private_ip` to convey to packer whether to SSH to the
public IP address of the intermediate machine or the private one.
Either way your security group needs to allow access to port 22.

## Maintenance mode
Members of the admin team on GitHub (number specified by the command-line flag
`-adminTeam`) can take Blueprint in and out of maintenance mode, during which
no modifications to the database are possible.

Caveat: After toggling maintenance mode, you will have to reload to see the
relevant UI changes, but the backend is locked down regardless.

## Improvements

 * Improve these docs!
 * Improve build.sh!
