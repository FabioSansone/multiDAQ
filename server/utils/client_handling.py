from dataclasses import dataclass, field
from server.utils.logger import get_logger

logger = get_logger('server_client_handling')

@dataclass
class BatchResult:
    successful: list[bytes] = field(default_factory=list)
    failed: list[bytes] = field(default_factory=list)
    
    @property
    def any_succeeded(self)->bool:
        return bool(self.successful)
    
    @property
    def all_succeeded(self)->bool:
        return not self.failed
    
    def run_per_client(items: list[bytes], action):
        result = BatchResult()
        for item in items:
            try:
                ok = action(item)
            except Exception:
                logger.exception(f"Unexpected exception while processing {item!r}")
                ok = False
            (result.successful if ok else result.failed).append(item) 
        
        return result