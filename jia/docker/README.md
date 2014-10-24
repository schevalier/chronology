#Deploying with Docker

## Quick usage HOWTO

We've already pushed a working [Dockerfile to Docker Hub](https://registry.hub.docker.com/u/chronology/jia/), so you can do something like the following:

  * On a host machine, put a `settings.py` in `/etc/jia/settings.py`
  * Run `sudo docker run -v /etc/jia:/etc/jia -p PUBLIC_PORT:JIA_PORT_IN_SETTINGS_PY chronology/jia:0.6.0`

## How to create new Dockerfiles for future versions of Jia

  * Copy an existing directory like `0.6.0` to the desired version
  * At a bare minimum, change the `RUN git checkout` in Dockerfile to the desired Chronology commit hash you wish to deploy.
  * Check the `Makefile` in this directory for some convenience tools, like running your new container.  For example, to build `1.0`, type `JIA_VERSION=1.0 make build`.
  * If you're bumping the version of Jia, update the default `JIA_VERSION` at the top of `Makefile`
