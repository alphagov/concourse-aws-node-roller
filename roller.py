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
    return frozenset(obj['name'] for obj in fly_blob if obj['state'] == 'landed')


def get_names(fly_blob):
    return frozenset(obj['name'] for obj in fly_blob)


def roll_workers():
    auto_scaling_group = client(
        service_name='autoscaling',
        region_name=REGION,
    )

    fly = Fly(concourse_url=CONCOURSE_URL, executable=FLY_PATH)
    fly.login(username='test', password='test', team_name='main')
    fly.run("sync")

    original_asg = client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[AUTOSCALING_GROUP_NAME],
    )["AutoScalingGroups"][0]
    original_instance_count = original_asg["DesiredCapacity"]
    print(f"ASG has original DesiredCapacity of {original_instance_count}")
    doubled_instance_count = original_instance_count * 2

    old_workers = fly.get_json("workers")
    print(f"Found {len(old_workers)} original workers")

    if original_instance_count != len(old_workers):
        raise RuntimeError("Original instance count doesn't match number of workers")

    print(f"Scaling ASG to {doubled_instance_count} instances.", end="")
    auto_scaling_group.set_desired_capacity(
        AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
        DesiredCapacity=doubled_instance_count,
        HonorCooldown=True,
    )

    for attempt in range(60):
        if len(fly.get_json("workers")) == doubled_instance_count:
            print('\n')
            break

        print(".", end='', flush=True)

        sleep(1)
    else:
        raise RuntimeError("Timed out waiting for workers to scale")

    old_workers = get_names(old_workers)
    all_workers = get_names(fly.get_json("workers"))

    print("-- OLD Workers --")
    print(old_workers)

    print('-- ALL Workers --')
    print(all_workers)

    print('-- NEW Workers --')
    new_workers = all_workers - old_workers
    print(new_workers)
    assert new_workers

    print("Landing old workers.", end="")
    for worker in old_workers:
        fly.run("land-worker", "--worker", worker)

    for attempt in range(60):
        landed_workers = get_landed_instances(fly.get_json("workers"))
        if len(landed_workers) == len(old_workers):
            print('\n')
            break

        print('.', end='', flush=True)

        sleep(1)
    else:
        raise RuntimeError("Timed out waiting for workers to land")

    print("Pruning old workers.")
    for worker in landed_workers:
        fly.run("prune-worker", "-w", worker)

    print("Scaling ASG back to {original_instance_count} instances.")
    auto_scaling_group.set_desired_capacity(
        AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
        DesiredCapacity=original_instance_count,
        HonorCooldown=True,
    )

    print("All good, now press CTRL+C")
