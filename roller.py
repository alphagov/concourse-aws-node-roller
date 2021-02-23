from fly import Fly
from boto3 import client
from time import sleep

# FLY_PATH = "fly"
FLY_PATH = "/home/work/bin/fly"

CONCOURSE_URL = "http://127.0.0.1:8080"
REGION = "eu-west-2"
AUTOSCALING_GROUP_NAME = "SCALING_GROUP"
DEFAULT_NUMBER_OF_WORKERS = 2
EXPECTED_WORKERS = 4
SLEEP_WAIT = 10


def get_landed_instances(fly_blob):
    landed_instances = []
    for obj in fly_blob:
        if obj['state'] == 'landed':
            landed_instances.append(obj['name'])
    return set(landed_instances)


def get_names(fly_blob):
    names = []
    for obj in fly_blob:
        names.append(obj['name'])
    return set(names)


def roll_workers():
    auto_scaling_group = client(
        service_name='autoscaling',
        region_name=REGION,
    )

    fly = Fly(concourse_url=CONCOURSE_URL, executable=FLY_PATH)
    fly.login(username='test', password='test', team_name='main')
    fly.run("sync")

    old_workers = fly.get_json("workers")

    pre_number_of_workers = len(old_workers)

    assert pre_number_of_workers == DEFAULT_NUMBER_OF_WORKERS, "Current number of workers: %s" % str(pre_number_of_workers)

    print("Pre number of workers: %s" % str(pre_number_of_workers))

    auto_scaling_group.set_desired_capacity(
        AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
        DesiredCapacity=EXPECTED_WORKERS,
        HonorCooldown=True,
    )

    print("Please wait while we scale")

    attempts = 0
    while True:
        if len(fly.get_json("workers")) == EXPECTED_WORKERS or attempts > 60:
            break

        print(".", end='', flush=True)

        sleep(1)
        attempts += 1

    print('\n')

    old_workers = get_names(old_workers)
    all_workers = get_names(fly.get_json("workers"))

    print("-- OLD Workers --")
    print(old_workers)
    print('-- ALL Workers --')
    print(all_workers)
    print('-- NEW Workers --')

    new_workers = list(all_workers - old_workers)

    print(new_workers)

    assert len(new_workers) > 0

    for worker in old_workers:
        fly.run("land-worker", "--worker", worker)

    attempts = 0
    successfully_landed_workers = False
    while True:
        if attempts > 60:
            print("Timing out!")
            break

        landed_workers = get_landed_instances(fly.get_json("workers"))

        if len(landed_workers) == len(old_workers):
            successfully_landed_workers = True
            break

        print('.', end='', flush=True)

        sleep(1)
        attempts += 1

    print('\n')

    if successfully_landed_workers:
        for worker in landed_workers:
            fly.run("prune-worker", "-w", worker)

        auto_scaling_group.set_desired_capacity(
            AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
            DesiredCapacity=DEFAULT_NUMBER_OF_WORKERS,
            HonorCooldown=True,
        )

        print("All good, now press CTRL+C")
    else:
        print("Problem while landing workers.")
