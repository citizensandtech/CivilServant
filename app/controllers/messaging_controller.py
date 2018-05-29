import praw
import inspect, os, sys # set the BASE_DIR
import simplejson as json
import datetime
import reddit.connection
import reddit.praw_utils as praw_utils
import reddit.queries
from app.models import Base, MessageLog
import app.event_handler
from collections import defaultdict, Counter

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
                account_message['subject'],
                account_message['message'], 
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
            self.log.info("Sending a message to user %s with data %s" % (username, str(body)))
            # NOTE: THIS CODE WAS ADJUSTED TO PREVENT ACTUAL MESSAGE SENDING
            # NOTE: FOR THE PURPOSE OF TESTING THE r/feminism experiment
            # WARNING: DO NOT COMMIT THESE TWO LINES
            # response = self.r.send_message(username, subject, body, raise_captcha_exception=True)
            response = {"errors":[]}
            # NOTE: END ALTERED CODE COMPONENT
 
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
