from enum import Enum

class report_status_enum(Enum): # Every Possible type should be declared here, if you added a function that check on type, add it here.
    SUCCESS = 1
    FAILED = -1
    IN_PROGRESS = 0
    SKIPPED = 2 
    INTERRUPTED = 3
    
class error_handling_enum(Enum):  # 100-1000 means Critical, > 1000 Means critical but consider it as success, < 10 means info , 10-100 means warning
    TRACK_REPORTER_NOT_FOUND = 113 # Track reporter not found in the function args
    ERROR_LOADING_BOTO3 = 107 #Permission failed to access AWS resources