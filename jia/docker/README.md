#Deploying with Docker

## Quick usage HOWTO

We've already pushed a working [Dockerfile to Docker Hub](https://registry.hub.docker.com/u/chronology/jia/), so you can do something like the following:

  * On a host machine, put a `settings.py` in `/etc/jia/settings.py`
  * Run `sudo docker run -v /etc/jia:/etc/jia -p PUBLIC_PORT:JIA_PORT_IN_SETTINGS_PY chronology/jia:0.6.0`

## How to create new Dockerfiles for future versions of Jia

  * Create a tag (e.g., v0.7.0) of the repository pointing to the right git commit of chronology/jia.
  * `JIA_VERSION=v0.7.0 make generate_dockerfiles`
  * `JIA_VERSION=v0.7.0 make build`. Note: Docker aggressively caches commands for performance.  To avoid this, for example if you had to change which hash a tag points to, call `JIA_VERSION=v0.7.0 make build_nocache`.
  * `JIA_VERSION=v0.7.0 make push`
