from fabric.api import task, local, run, cd, put, sudo, env

env.use_ssh_config = True


@task(default=True)
def deploy():
    with cd('/srv/iot-adapter'):
        sudo('chmod -R a+w .')
        put('*', '.')
        sudo('docker build -t i-maintenance/iot-adapter .')

        # stop old container, if it doesn't exist also return true
        sudo('docker rm -f iot-adapter || true')

        # start new container
        # sudo docker run -dit --restart always -e "LOGSTASH_HOST=il060" -e "LOG_LEVEL=DEBUG" --name iot-adapter i-maintenance/iot-adapter
        sudo('docker run '
            '-dit '
            '--restart always '
            '-e "LOGSTASH_HOST=il012" '
            '-e "LOG_LEVEL=DEBUG" '
            '--name iot-adapter '
            'i-maintenance/iot-adapter')


@task
def logs():
    sudo('docker logs -f iot-adapter')

@task
def stop():
    sudo('docker rm -f iot-adapter')