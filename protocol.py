from collections import namedtuple
from typing import List

from event_graph import EventGraph

Dataset = namedtuple("Dataset", "spans profiles events")
Telemetry = namedtuple("Telemetry", "evrs eha")
Directive = namedtuple("Directive", "type start_time args")
Span = namedtuple("Span", "start_time duration")
Profile = namedtuple("Profile", "name segments")
Segment = namedtuple("Segment", "start_time value")

Delay = namedtuple("Delay", "duration")
AwaitCondition = namedtuple("AwaitCondition", "condition")
Call = namedtuple("Call", "child_task args")
Spawn = namedtuple("Spawn", "child_task")
Completed = namedtuple("Completed", "")

class Plan:
    def __init__(self, directives):
        self.directives: List[Directive] = list(directives)

def tuple_args(args):
    return tuple(sorted(args.items()))

def untuple_args(tupled_args):
    return {k: v for k, v in tupled_args}

def hashable_directive(directive):
    return Directive(directive.type, directive.start_time, tuple_args(directive.args))

def restore_directive(hashable_directive):
    return Directive(hashable_directive.type, hashable_directive.start_time, untuple_args(hashable_directive.args))
