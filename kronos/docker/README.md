#Deploying with Docker

## Quick usage HOWTO

We've already pushed a working [Dockerfile to Docker Hub](https://registry.hub.docker.com/u/chronology/kronos/), so you can do something like the following:

  * On a host machine, put a `settings.py` in `/etc/kronos/settings.py`
  * Create `/var/log/kronos` for log output
  * Run `sudo docker run -d -p 8550:8150 -p 8551:8151 -p 8552:8152 -v /etc/kronos/settings.py:/etc/kronos/settings.py -v /var/log/kronos:/var/log/kronos chronology/kronos:$(KRONOS_VERSION)`

## How to create new Dockerfiles for future versions of Kronos

  * Tag the git commit for which you want to generate a Docker.
  * In `kronos/docker` directory, run `(export KRONOS_VERSION=v0.7.0; make generate_dockerfiles)`
  * In the same directory, run `(export KRONOS_VERSION=v0.7.0; make build)`. Note: Docker aggressively caches commands for performance.  To avoid this, for example if you had to change which hash a tag points to, call `(export KRONOS_VERSION=v0.7.0; make build_nocache)`.
  * In the same directory, run `(export KRONOS_VERSION=v0.7.0; make push)`
