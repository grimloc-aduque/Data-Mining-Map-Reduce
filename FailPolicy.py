

from random import random


class FailPolicy:
    map_might_fail =  True
    reduce_might_fail =  True
    master_will_fail = False
    fail_rate = 0.25

    master_did_fail = False

    def failure_on_coordinator():
        return FailPolicy.master_will_fail

    def failure_on_map():
        if FailPolicy.map_might_fail:
            return FailPolicy.random_failure()

    def failure_on_reduce():
        if FailPolicy.reduce_might_fail:
            return FailPolicy.random_failure()

    def random_failure():
        if random() <= FailPolicy.fail_rate:
            return True