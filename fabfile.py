from fabric.api import task, local, run, cd, put, sudo, env

env.use_ssh_config = True


@task(default=True)
def deploy():
    with cd('/srv/iot-adapter'):
        put('*', '.')
        run('docker build -t i-maintenance/iot-adapter .')

        # stop old container
        run('docker stop iot-adatper')
        run('docker rm iot-adatper')

        # start new container
        run('docker run '
            '-dit '
            '--restart always '
            '-e "LOGSTASH_HOST=il012" '
            '-e "LOG_LEVEL=DEBUG" '
            '--name iot-adatper '
            'i-maintenance/iot-adapter')


@task
def logs():
    run('docker logs -f iot-adatper')
