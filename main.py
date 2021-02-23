from moto import mock_autoscaling, mock_ec2

from boto3 import client, resource

from docker import from_env

from os import environ
from subprocess import PIPE, Popen
from threading import Thread
from signal import signal, SIGINT

from requests import get
from requests.exceptions import ConnectionError

from time import sleep

from roller import roll_workers

AUTOSCALING_GROUP_NAME = "SCALING_GROUP"
REGION = "eu-west-2"
SLEEP_WAIT = 10

DEFAULT_NUMBER_OF_WORKERS = 2
EXPECTED_WORKERS = 4

CONCOURSE_URL = "http://127.0.0.1:8080"
# COMPOSE_PATH = "docker-compose"
COMPOSE_PATH = "/home/work/.pyenv/shims/docker-compose"


def number_of_docker_workers():
    docker = from_env()

    containers = docker.containers.list()

    total = 0

    for container in containers:
        container_name = container.name  # node_roller_worker_1
        index_of_last_underscore = container_name.rindex('_')
        if container_name.endswith('_worker', 0, index_of_last_underscore):
            total += 1

    return total


def setup_mocked_as_group():
    mocked_as = mock_autoscaling()

    change_capacity_old = mocked_as.backends[REGION].change_capacity
    set_desired_capacity_old = mocked_as.backends[REGION].set_desired_capacity

    def change_capacity(self, group_name, scaling_adjustment):
        print("Hello from: change_capacity")

        change_capacity_old(self, group_name, scaling_adjustment)

    def set_desired_capacity(self, new_capacity):
        print("Setting new desired capacity: %s" % str(new_capacity))

        if number_of_docker_workers() > new_capacity:
            run_docker_compose_cmd("up -d --scale worker=%s --no-recreate" % str(new_capacity))

        set_desired_capacity_old(self, new_capacity)

    mocked_as.backends[REGION].change_capacity = change_capacity
    mocked_as.backends[REGION].set_desired_capacity = set_desired_capacity

    mocked_as.start(reset=False)


# Taken from: https://github.com/spulec/moto/blob/0912abe5f62bd6a10ae47656040e5d14ee5f804e/tests/test_autoscaling/utils.py#L8
@mock_ec2
def setup_networking():
    ec2 = resource("ec2", region_name=REGION)
    vpc = ec2.create_vpc(CidrBlock="10.11.0.0/16")
    subnet1 = ec2.create_subnet(VpcId=vpc.id, CidrBlock="10.11.1.0/24", AvailabilityZone=REGION + "a")
    subnet2 = ec2.create_subnet(VpcId=vpc.id, CidrBlock="10.11.2.0/24", AvailabilityZone=REGION + "b")
    return {"vpc": vpc.id, "subnet1": subnet1.id, "subnet2": subnet2.id}


def setup_mock_auto_scaling_group():
    setup_mocked_as_group()

    vpc_ids = setup_networking()

    ag = client(
        service_name='autoscaling',
        region_name=REGION,
    )

    ag.create_launch_configuration(
        LaunchConfigurationName="tester",
        ImageId="1234567",
        InstanceType="t2.medium",
    )

    ag.create_auto_scaling_group(
        AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
        MaxSize=DEFAULT_NUMBER_OF_WORKERS,
        MinSize=DEFAULT_NUMBER_OF_WORKERS,
        LaunchConfigurationName="tester",
        VPCZoneIdentifier=vpc_ids['subnet1']
    )


def run_docker_compose_instance():
    run_docker_compose_cmd("up --scale worker=%s --no-recreate" % str(DEFAULT_NUMBER_OF_WORKERS))


def run_docker_compose_cmd(command):
    env = {}

    # for key in environ.keys():
    #     if key.lower().startswith("docker_"):
    #         env[key] = environ[key]

    compose_process = Popen(COMPOSE_PATH + " " + command, stdin=PIPE, stdout=PIPE, shell=True, env=env)

    while True:
        print(compose_process.stdout.read().decode('utf-8'))

        if compose_process.poll() is not None:
            break


def wait_for_concourse():
    while True:
        try:
            status = get(CONCOURSE_URL)

            if status.status_code == 200:
                break
        except ConnectionError:
            pass

        sleep(SLEEP_WAIT)

    print("Concourse is up!")
    sleep(SLEEP_WAIT)
    setup_mock_auto_scaling_group()
    roll_workers()


def main():
    docker_compose_thread = Thread(target=run_docker_compose_instance)
    # concourse_checker_thread = Thread(target=wait_for_concourse)

    docker_compose_thread.start()
    # concourse_checker_thread.start()

    wait_for_concourse()

    # Handle ctrl + c gracefully
    signal(SIGINT, lambda sig, frame: run_docker_compose_cmd("down"))


main()

#
# described_group = ag.describe_auto_scaling_groups(AutoScalingGroupNames=[AUTOSCALING_GROUP_NAME])
#
# assert described_group['AutoScalingGroups'][0]['DesiredCapacity'] == 4