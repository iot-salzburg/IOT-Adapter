from fabric.api import task, local, run, cd, put, sudo, env

env.use_ssh_config = True


@task(default=True)
def deploy():
    with cd('/home/cschranz/iot-adapter'):
        put('*', '.')
        run('sudo docker build -t i-maintenance/iot-adapter .')

        # stop old container
        run('sudo docker rm -f iot-adatper')

        # start new container
        # sudo docker run -dit --restart always -e "LOGSTASH_HOST=il060" -e "LOG_LEVEL=DEBUG" --name iot-adatper i-maintenance/iot-adapter
        run('sudo docker run '
            '-dit '
            '--restart always '
            '-e "LOGSTASH_HOST=il060" '
            '-e "LOG_LEVEL=DEBUG" '
            '--name iot-adatper '
            'i-maintenance/iot-adapter')


@task
def logs():
    run('sudo docker logs -f iot-adatper')
