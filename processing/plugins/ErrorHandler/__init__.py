import os
from datetime import datetime
import json
from functools import wraps 
import sys 

# Import Mother Class : 
from plugins.ErrorHandler.enums import report_status_enum, error_handling_enum
from plugins.NotifcationReporter import NotificationReporter

"""

ErrorHandler Module

-------------------

This will be the responsible of handling error in a custom way during this whole project, some errors will have some definied actions, others will be just reported to our SQS queue.

"""

class ErrorHandler :
    """
    ErrorHandler is the main class used for this module
    """
    def __init__(self, track_reporter: NotificationReporter = None) :
        """
        Constructor for ErrorHandler class
        
        :param track_reporter: The reporter object to be used to report the errors.
        
        :type track_reporter: NotificationReporter
        """
        self._track_reporter = track_reporter
    
    def set_track_reporter(self, args: list) -> None:
        """
        set_track_reporter is a tricky function, it's will set the track_reporter object from the function args 
        which is going to be the class itself, this is used to avoid passing the track_reporter from step to step.  
        
        :param args: function inputs, most of case will be (self,...) and we will only get the self class
        
        :type track_reporter: list
        """
        if args  :
            function_class = args[0] # Get first args which is going to be self class
            self._track_reporter = function_class.track_reporter # Get the track_reporter object from the class, carefull this may crash if not fund, maybe add try except or a previous check?
        else :
            raise Exception(error_handling_enum.TRACK_REPORTER_NOT_FOUND.value)
        
        
    def handle_error(self, func) :
        """
        handle_error is the function that will be used as a decorator to handle errors in a custom way.
        
        :param self: The object itself.
        :param func: The function to be decorated.
        
        :type self: ErrorHandler
        :type func: function
        
        :return: The decorated function.
        :rtype: function
        """
        @wraps(func) # This to not overwrite the function name and docstring
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                
            except Exception as e: # You may want to add more exceptions type later to be more specific.
                print(f"Error: {e}")
                
                # Get error name from the error code if exist : 
                try :
                    trace_logs = error_handling_enum(int(str(e))).name
                except :
                    trace_logs = str(e); e = -1
                    
                # Check if we have a reporter object to report the error, if not we'll get it from the function args.
                if not self._track_reporter :
                    print("kwargs", kwargs)
                    self.set_track_reporter(args)
                
                # Reporting checkpoint
                reporting_tracker_message = {
                    "error_code" : str(e),
                    "trace_logs" : trace_logs,
                    "function_failed" : func.__name__,
                    "status" : report_status_enum.FAILED.value, 
                }
                
                res = self._track_reporter.publish_to_sqs(reporting_tracker_message, func.__name__)
                
                try : 
                    if int(str(e)) > 10000 : continue_anyways = True # If it's a soft critical error we can consider it as success
                    else : continue_anyways = False
                except : continue_anyways = False
                
                if not continue_anyways : # Maybe if you want more trace you can disable the default one and print the "trace_logs" or play with it as you wish, remember e here is the error code.
                    raise Exception(e)
                
                else: # if error code > 10000, means the error is used only to stop the process, no need to consider it as error, it's a success execution.
                    sys.exit(0)
                    
            return result
            
        return wrapper