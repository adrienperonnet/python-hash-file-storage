#!/usr/bin/env python

class HfileException(Exception):
    def __init__(self, message="",data_object=None,original_message=None):
        #The object in which the exception was raised
        self.data_object=data_object
        #The message add in hfile
        self.message=message
        #If this exception was raised by another exception, keep a trace of the old message
        self.original_message=original_message

    def __str__(self):
        return "%s on %s (%s)"%(self.message,self.data_object,self.original_message)

    def dump(self):
        ret={
            'class':self.data_object.__class__.__name__,
            'object_path':self.data_object._get_object_path(),
            'infos_field':list(self.data_object.__class__.infos_fields),
            'message':self.message,
            }
        if self.original_message is not None:
            ret["original_message"]=self.original_message.message
        return ret
        
class ObjectNotFound(HfileException):
    """
    Can't find the data object
    """
    pass

class ObjectMalformed(HfileException):
    """
    The data object is not well parsed
    """
    pass




