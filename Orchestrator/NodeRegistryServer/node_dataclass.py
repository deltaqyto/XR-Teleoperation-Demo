from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class LifeStatus:
    status: str
    reason: Optional[str]
    last_seen: float


@dataclass
class ChangeFlags:
    config_schema: bool = False
    command_schema: bool = False


class Node:
    def __init__(self, node_name: str, node_id: str, message_time: float):
        self.node_name = node_name
        self.node_id = node_id
        self.payload_queue: List[Any] = []
        self.config_schema: Optional[Dict] = None
        self.command_schema: Optional[Dict] = None
        self.change_flags = ChangeFlags()
        self.last_message_time = message_time
        self.life_status = LifeStatus(status='alive', reason=None, last_seen=message_time)
