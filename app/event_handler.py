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

"""
the exposed method

To use, put this decorator on the method that is expecting to run callbacks:
	@event_handler()
(The args "NameOfThisClass", "name_of_this_method" should match the class/method names in this file (obviously), as
well as the class/method names in the EventHook table.)

"""
def event_handler(func):
	@wraps(func)
	def wrapper(instance, *args, **kwargs):
		# first run callbacks with EventWhen.BEFORE
		run_callbacks(instance, EventWhen.BEFORE)

		# run the target function (that this event_handler is decorating)
		response = func(instance, *args, **kwargs)

		# first run callbacks with EventWhen.AFTER
		run_callbacks(instance, EventWhen.AFTER)
		return response
	return wrapper


def run_callbacks(instance, call_when):
	caller_controller = instance.__class__.__name__

	# caller_method is first function as you step through the frames that is a class method of caller_controller
	caller_method = get_caller_method_name(instance)

	now = datetime.datetime.utcnow()

	#query for callbacks that we should run
	events = instance.db_session.query(EventHook).filter(
		and_(EventHook.caller_controller == caller_controller, 
			EventHook.caller_method == caller_method,
			EventHook.call_when == call_when.value,
			EventHook.is_active == True)
		).all()

	instance.log.info("EVENTS: {0}".format(events))

	# get callbacks that are part of active experiments
	experiment_ids = set([e.experiment_id for e in events])
	experiments = []
	if len(experiment_ids) > 0:
		experiments = instance.db_session.query(Experiment).filter(Experiment.id.in_(list(experiment_ids))).all()
	experiment_states = {e.id: (now > e.start_time and now < e.end_time) for e in experiments}
	active_events = [e for e in events if (e.experiment_id and e.experiment_id in experiment_states and experiment_states[e.experiment_id])]

	experiment_info = {e.id: e for e in experiments}

	# instance (caller instance) must pass in instances of all callbacks' controllers	
	class_to_instance = parse_instances(instance)
	for e in active_events:
		callee_instance = get_instance("{0}.{1}".format(e.callee_module, e.callee_controller), class_to_instance)
		if callee_instance:
			callee_method = getattr(callee_instance, e.callee_method)
			callee_method(instance)	# callee methods always only take in 1 arg: instance
		else:
			instance.log.error("Error in event_handler: callee_instance not found to be passed in caller_instance {0}.".format(instance))


"""
given 
	an instance of a class that takes in "instance_*" args, 

parse those args to get their values' classes

returns 
	dictionary of {arg_name: class_name}
"""
def parse_instances(instance):
	class_to_instance = {}
	args = inspect.signature(instance.__class__).parameters # dict {parameter_name: Parameter}
	for arg in args:
		if arg and "instance_" in arg:
			class_name = arg[len("instance_"):]
			class_to_instance[class_name] = getattr(instance, arg)
	return class_to_instance

"""
given
	class name
	class_to_instance dictionary (output of function parse_instances)

returns
	instance of that class
"""
def get_instance(name, class_to_instance):
	if name in class_to_instance:
		return class_to_instance[name]
	else:
		for class_name in class_to_instance:
			# allowing for different bindings of name
			if name in class_name or class_name in name:
				return class_to_instance[class_name]
		return None

# finds the 1st method from the outer frames that is an attribute of the given instance's class
def get_caller_method_name(instance):
	frames = inspect.getouterframes(inspect.currentframe())
	for frame in frames:
		try:
			callee_method = getattr(instance, frame.function)
			return frame.function
		except AttributeError:
			pass
	instance.log.error("Error while looking for caller method name that is in class {0}".format(instance.__class__.__name__))