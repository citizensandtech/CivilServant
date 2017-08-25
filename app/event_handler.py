"""
This event handler checks for EventHooks that should be executed, before or after a method

experiment_controller.py should have the callback code
experiment.yml should contain info about what should be added to EventHook table

To use, put this decorator on the method that is expecting to run callbacks:
    @event_handler()
(The args "NameOfThisClass", "name_of_this_method" should match the class/method names in this file (obviously), as
well as the class/method names in the EventHook table.)

event_handler will check for callbacks to run before and after the method. 
It has access to all of the attributes/methods of its instance's class. 
    So to expose/"pass in" a variable to the event_handler, make it a class attribute (self.my_data = my_data).
    So if you need to access the db_session or log, use instance.db_session or instance.log

callbacks should all pass in instance (self)

"""

from utils.common import EventWhen
from app.models import Base, EventHook, Experiment
from sqlalchemy import and_
import datetime
from functools import wraps
import importlib
import inspect
import simplejson as json

"""
the exposed method

To use, put this decorator on the method that is expecting to run callbacks:
    @event_handler()

"""
def event_handler(func):
    @wraps(func)
    def wrapper(instance, *args, **kwargs):
        # first run callbacks with EventWhen.BEFORE
        run_callbacks(instance, func.__name__, EventWhen.BEFORE)

        # run the target function (that this event_handler is decorating)
        response = func(instance, *args, **kwargs)

        # first run callbacks with EventWhen.AFTER
        run_callbacks(instance, func.__name__, EventWhen.AFTER)
        return response
    return wrapper


def run_callbacks(instance, caller_method, call_when):
    if getattr(instance, "experiment_to_controller", None) is None:
        initialize_callee_controllers(instance)

    caller_controller = instance.__class__.__name__

    now = datetime.datetime.utcnow()

    #query for callbacks that we should run
    events = instance.db_session.query(EventHook).filter(
        and_(EventHook.caller_controller == caller_controller, 
            EventHook.caller_method == caller_method,
            EventHook.call_when == call_when.value,
            EventHook.is_active == True)
        ).all()

    # get callbacks that are part of active experiments
    experiment_ids = set([e.experiment_id for e in events])
    experiments = []
    if len(experiment_ids) > 0:
        experiments = instance.db_session.query(Experiment).filter(Experiment.id.in_(list(experiment_ids))).all()
    experiment_states = {e.id: (now > e.start_time and now < e.end_time) for e in experiments}
    active_events = [e for e in events if (e.experiment_id and e.experiment_id in experiment_states and experiment_states[e.experiment_id])]

    experiment_info = {e.id: e for e in experiments}

    # no guaranteed order that events are run
    for e in active_events:
        callee_instance = instance.experiment_to_controller[e.experiment_id] if e.experiment_id in instance.experiment_to_controller else None 
        if callee_instance:
            callee_method = getattr(callee_instance, e.callee_method)
            callee_method(instance) # callee methods always only take in 1 arg: instance
        else:
            instance.log.error("Error in event_handler: callee_instance not found to be passed in caller_instance {0}.".format(instance))


"""
create experiment_to_controller attribute for instance
experiment_to_controller is a dictionary of experiment.id to callee_controller_instance 
"""
def initialize_callee_controllers(instance):
    instance.experiment_to_controller = {}
    caller_controller = instance.__class__.__name__
    experiments = instance.db_session.query(Experiment).all()
    for experiment in experiments:
        settings = json.loads(experiment.settings_json)
        if('event_hooks' not in settings.keys()):
            continue
        for hook_name in settings["event_hooks"]:
            if settings["event_hooks"][hook_name]["caller_controller"] == caller_controller:
                callee_module_name = settings["event_hooks"][hook_name]["callee_module"]
                callee_controller_name = settings["event_hooks"][hook_name]["callee_controller"]
                
                callee_module = importlib.import_module(callee_module_name)                
                callee_controller = getattr(callee_module, callee_controller_name)
                callee_controller_instance = callee_controller(experiment.name, instance.db_session, instance.r, instance.log)

                instance.experiment_to_controller[experiment.id] = callee_controller_instance
