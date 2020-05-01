#!/usr/bin/env python3

# Use this with any experiment meant to be facilitated by the event handler. If
# this makes use of recurring experiment-specific jobs, schedule them using
# schedule_experiments.py instead.

import argparse
import ast
import importlib.util
import os
from pathlib import Path

import yaml

from app.controller import BASE_DIR, ENV, conn, db_session, log

CONTROLLERS_MODULE_BASE_NAME = 'app.controllers'
CONTROLLERS_PATH = Path(BASE_DIR)/'app'/'controllers'
LOG_PREFIX = '%s:' % str(Path(__file__).stem)


def collect_experiment_controller_classes():
    """Collect class defs from the AST of the experiment controller modules."""
    # The AST is used so the names of each available experiment controller
    # class can be fetched without the need to instantiate each controller
    # module and otherwise unintentionally run experiment-related code
    try:
        controller_classes = {}
        for controller_mod_path in CONTROLLERS_PATH.glob('**/*experiment*.py'):
            with open(str(controller_mod_path)) as f:
                module_ast = ast.parse(f.read())
            controller_classes.update({node.name:controller_mod_path
                for node in module_ast.body if isinstance(node, ast.ClassDef)})
        return controller_classes
    except:
        log_msg = '%s Error collecting experiment controller classes.'
        log.error(log_msg, LOG_PREFIX)
        raise


def extract_module_name_from_path(module_path):
    """Get the module name from the path and format for importlib usage."""
    try:
        abs_base_path = Path(BASE_DIR).resolve()
        abs_mod_path = module_path.resolve()
        rel_mod_path = Path(str(abs_mod_path).replace(str(abs_base_path), ''))
        return str(rel_mod_path.with_suffix('')).replace(os.path.sep, '.')
    except:
        log_msg = '%s Error extracting the module name from the module path: '
        log.error(log_msg, LOG_PREFIX, module_path)
        raise


def import_experiment_controller_class(class_name, module_path):
    """Import the experiment controller class from the provided module path."""
    try:
        module_name = extract_module_name_from_path(module_path)
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, class_name)
    except:
        log_msg = '%s Error importing the experiment controller class: %s'
        log.error(log_msg, LOG_PREFIX, class_name)
        raise


def get_experiment_controller_class(experiment_name):
    try:
        experiment_config = load_experiment_config(experiment_name)
        controller_name = experiment_config[ENV]['controller']
        controller_classes = collect_experiment_controller_classes()
        controller_module_path = controller_classes[controller_name]
        return import_experiment_controller_class(
            controller_name, controller_module_path)
    except:
        log_msg = '%s Error geting the experiment controller class: %s'
        log.error(log_msg, LOG_PREFIX, experiment_name)
        raise


def load_experiment_config(experiment_name):
    """Load the configuration file for the provided experiment name."""
    try:
        path = Path(BASE_DIR)/'config'/'experiments'/(experiment_name+'.yml')
        with open(str(path)) as f:
            config = yaml.full_load(f)
        return config
    except:
        log_msg = '%s Error reading the experiment configuration: %s'
        log.error(log_msg, LOG_PREFIX, experiment_name)
        raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env',
                        default=ENV,
                        help=('name of the CivilServant environment where the '
                              'experiment will be deployed (defaults to '
                              '$CS_ENV)'))
    parser.add_argument('experiment_name',
                        help='name of the experiment to initialize')
    return parser.parse_args()


def run_experiment(experiment_name):
    """Run the specified experiment."""
    try:
        controller_class = get_experiment_controller_class(experiment_name)
        r = conn.connect(controller=experiment_name)
        controller = controller_class(
            experiment_name = experiment_name,
            db_session = db_session,
            r = r,
            log = log)
        log_msg = '%s Successfully started experiment: %s, ID: %d'
        log.info(log_msg, LOG_PREFIX, experiment_name,
            controller.experiment.id)
    except:
        log_msg = '%s Error running the experiment: %s'
        log.exception(log_msg, LOG_PREFIX, experiment_name)
        raise
        

if __name__ == '__main__':
    try:
        args = parse_args()
        ENV = os.environ['CS_ENV'] = args.env
        run_experiment(args.experiment_name)
    except:
        # Exceptions handled and logged in run_experiment()
        pass
