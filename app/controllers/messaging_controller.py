import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import os
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from pathlib import Path
from app.models import Base, ExperimentAction, MessageLog
import app.event_handler
from utils.common import ThingType
from collections import defaultdict, Counter

ENV = os.environ["CS_ENV"]
SURVEY_LOG_PATH = Path(__file__) / ".." / ".." / ".." / "logs" / ("surveyed_users_%s.log" % ENV)
SURVEY_LOG_PATH = str(SURVEY_LOG_PATH.resolve())

class MessageError(Exception):
    def __init__(self, message, errors = []):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        # Now for your custom code...
        #self.errors = errors


class MessagingController:
    def __init__(self, db_session, r, log):
        self.db_session = db_session
        self.log = log
        self.r = r
        self.platform = "reddit"

    ## SEND A MESSAGE TO AN ACCOUNT AND LOG THE OUTCOME USING log_metadata
    ## account_messages should be in the format:
    ## [ {"account": "account_name", 
    ##    "subject":"subject text", 
    ##    "message": "message text"}]
    ## This method will return a dict of results. 
    ## This method will raise a MessageError error if duplicate accounts are submitted
    ## TODO: TEST THAT THE SUBJECT IS PROPERLY ASSIGNED
    def send_messages(self, account_messages, message_task_id, log_metadata = None):
        recipient_accounts = dict(Counter([x['account'] for x in account_messages]))
        duplicate_accounts = {k:v for (k,v) in recipient_accounts.items() if v>1}
        if(len(duplicate_accounts)>0):
            raise MessageError(["Duplicate accounts submitted to send_messages.",
                                duplicate_accounts])

        log_results = {}
        for account_message in account_messages:
            log_results[account_message['account']] = self.send_message(
                account_message['account'], 
                account_message['message'], 
                account_message['subject'],
                message_task_id,
                log_metadata)
        return log_results

    ## SEND A MESSAGE TO AN ACCOUNT AND LOG THE OUTCOME
    ## Return information about the outcome:
    ##    - success: message sent
    ##    - failure: message failed to send for another reason

    def send_message(self, username, body, subject, message_task_id, log_metadata=None):
        message_sent = None
        response = {"errors": []}
        try:
            self.log.info("Sending a message to user %s with subject %s" % (username, str(subject)))
            # NOTE: THIS CODE WAS ADJUSTED TO PREVENT ACTUAL MESSAGE SENDING
            # NOTE: FOR THE PURPOSE OF TESTING THE r/feminism experiment
            # WARNING: DO NOT COMMIT THIS LINE UNCOMMENTED
            # response = {"errors":[]}
            # NOTE: END ALTERED CODE COMPONENT
            response = self.r.send_message(username, subject, body, raise_captcha_exception=True)
 
            if response["errors"] and len(response['errors'])>0:
                self.log.error("Error in response when sending a message to reddit account %s: %s" % (username, str(response)))
                message_sent = False
            else:
                self.log.info("Message successfully sent to reddit account %s" % username)
                message_sent = True
        except praw.errors.InvalidUser as e:
            self.log.exception("Failed to send message to reddit account %s because user doesn't exist" % username)
            self.log.error(e.response)
            message_sent = False
            response["errors"].append({"username":username, "error": "invalid username"})
        except praw.errors.InvalidCaptcha as e:
            self.log.exception("Message sending on reddit requires a captcha")
            self.log.error(e.response)
            message_sent = False
            response["errors"].append({"username":username, "error": "invalid captcha"})
        except Exception as e:
            self.log.exception("Failed to send reddit message to %s" % username)
            message_sent = False
            response["errors"].append({"username":username, "error": "general exception"})
        
        ## ONLY LOG THE MESSAGE IF IT WAS SENT
        if(message_sent):
            message_log = MessageLog(created_at = datetime.datetime.utcnow(),
                                platform=self.platform,
                                username = username,
                                subject = subject,
                                body = body,
                                message_sent = True,
                                message_task_id = message_task_id,
                                metadata_json = log_metadata)
        else:
            message_log = MessageLog(created_at = datetime.datetime.utcnow(),
                    platform=self.platform,
                    username = username,
                    subject = subject,
                    body = body, 
                    message_sent = False,
                    message_failure_reason = response['errors'][-1]['error'],
                    message_task_id = message_task_id,
                    metadata_json = log_metadata)
        self.db_session.add(message_log)
        return response

    ## FIND ALL PREVIOUS MESSAGE SENDING ATTEMPTS ASSOCATED WITH AN ACCOUNT
    ## FILTERED OPTIONALLY BY MESSAGE TASK ID
    def get_previous_messages(self, username, message_task_id=None):
        pass

class SurveyController:
    def __init__(self, db_session, r, log, experiment_controller):
        self.db_session = db_session
        self.log = log
        self.r = r
        self.experiment_controller = experiment_controller
        
        settings = experiment_controller.experiment_settings
        self.survey_subject = settings["survey_message_subject"]
        self.survey_text = settings["survey_message_text"]
        self.survey_url = settings["survey_url"]
        self.survey_task_id = "SurveyController(name)".format(
            name=experiment_controller.experiment_name)
        
        self.messaging_controller = MessagingController(
            db_session = db_session,
            r = r,
            log = log
        )

    def _update_metadata(self, exp_object, key, value):
        metadata = {}
        if exp_object.metadata_json:
            metadata = json.loads(exp_object.metadata_json)
        metadata[key] = value
        exp_object.metadata_json = json.dumps(metadata)

    def append_surveyed_user_log(self, username, response):
        invalid_username = False
        sent_status = True
        url = self.survey_url.format(username=username)
        for error in response.get("errors", []):
            sent_status = False
            if error["error"] == "invalid username":
                invalid_username = True
                break
        entry = [username,
                 invalid_username,
                 datetime.datetime.utcnow(),
                 False if not invalid_username else None,
                 url,
                 sent_status
        ]
        entry = ",".join([str(col) for col in entry]) + "\n"
        with open(SURVEY_LOG_PATH, "a+") as f:
            f.write(entry)
                    
    def get_unique_recipient_message_dicts(self, user_things):
        # Dictionary used to guarantee uniqueness rather than a set in order
        # to maintain insertion order in case this is presumed by the caller
        unique_recipients = {}
        for user_thing in user_things:
            url = self.survey_url.format(username=user_thing.thing_id)
            message = self.survey_text.format(
                username = user_thing.thing_id,
                url = url
            )
            unique_recipients[user_thing.thing_id] = {
                "account": user_thing.thing_id,
                "subject": self.survey_subject,
                "message": message
            }
        return list(unique_recipients.values())

    def send_surveys(self):
        experiment_id = self.experiment_controller.experiment.id
        experiment_name = self.experiment_controller.experiment.name
        
        try:
            with self.db_session.cooplock(self.survey_task_id, experiment_id):
                user_things = self.experiment_controller.get_surveyable_user_things()
                self.log.info("Experiment {0}: identified {1} surveyable users.".format(
                    experiment_name, len(user_things)))
                self.log.info("Experiment {0}: messaging users: {1}".format(
                    experiment_name, [user_thing.thing_id for user_thing in user_things]))
                message_dicts = self.get_unique_recipient_message_dicts(user_things)
                responses = self.messaging_controller.send_messages(message_dicts, self.survey_task_id)
                
                updates = []
                for user_thing in user_things:
                    if user_thing.thing_id in responses.keys():
                        response = responses[user_thing.thing_id]
                        self.append_surveyed_user_log(user_thing.thing_id, response)
                        if len(response.get("errors", {})) > 0:
                            self.log.info("Experiment {0}: failed to survey user {1}: {2}".format(
                                experiment_name, user_thing.thing_id, str(response)))
                            self._update_metadata(user_thing, "survey_status", "failed")
                            updates.append(user_thing)
                        else:
                            self.log.info("Experiment {0}: successfully surveyed user {1}: {2}".format(
                                experiment_name, user_thing.thing_id, str(response)))
                            survey_action = ExperimentAction(
                                experiment_id = experiment_id,
                                action = "SendSurvey",
                                action_object_type = ThingType.USER.value,
                                action_object_id = user_thing.thing_id
                            )
                            self._update_metadata(survey_action, "survey_status", "sent")
                            self._update_metadata(user_thing, "survey_status", "sent")
                            updates.append(survey_action)
                            updates.append(user_thing)
                if updates:
                    self.db_session.add_retryable(updates)
        except Exception as e:
            self.log.exception("Experiment {0}: error occurred while sending surveys: ".format(
                experiment_name))

