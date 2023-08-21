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
