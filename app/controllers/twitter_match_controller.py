import datetime

from app.controllers.twitter_controller import TwitterController
from app.models import Base, TwitterUser, TwitterStatus, LumenNoticeToTwitterUser, TwitterUserSnapshot, TwitterFill, \
    TwitterUnshortenedUrls, TwitterStatusUrls, ExperimentThing
from sqlalchemy import and_, or_, func, distinct
import utils.common

TWITTER_DATETIME_STR_FORMAT = "%a %b %d %H:%M:%S %z %Y"


class TwitterMatchController(TwitterController):
    def __init__(self):
        super.__init__()
        self.now = datetime.datetime.utcnow()
        self.period_ago = self.now - datetime.timedelta(
            days=self.config['match_criteria']['random_user_active_within_days'])
        self.match_id = self.now.strftime('%Y%m%d%H%M%S')

    def match_lumen_and_random_id_users(self, batch_size=100):
        """Select all the twitter_users that are not part of and experiment group and group them
        - want to match random_id_users who tweeted most K-hours (say 48) within the the lumen notice date as the
        - so the date range is essentially the block.
        - want the comparison (random-id) group to be larger maybe 1.25 to 1.5 times the lumen-group.
        - report how large the groups are"""

        # DMCA notice offense was issued
        # doesn't need to do direct pair matching, can be around
        # are we blocking? no

        unmatched_users = self.get_unmatched_users()

        valid_unmatched_users, invalid_unmatched_users = self.filter_matchable_users(unmatched_users)

        self.invalidate_not_qualifying_users(invalid_unmatched_users)

        matches, still_unmatched = self.make_matches(valid_unmatched_users)

        self.log.info('There were {} matches and {} left unmatched'.format(len(matches), len(still_unmatched)))

        self.save_matches(matches)

        self.report_matching_status()

        num_matched = len(matches)
        return num_matched

    def get_unmatched_users(self):
        join_clause = and_(TwitterUser.id == ExperimentThing.query_index)
        filter_clause = and_(ExperimentThing.id == None,
                             TwitterUser.user_state == utils.common.TwitterUserState.FOUND.value)  # want to find the users that have not been matched and eligible

        unmatched_users_ret = self.db_session.query(TwitterUser, ExperimentThing) \
            .outerjoin(ExperimentThing, join_clause) \
            .filter(filter_clause) \
            .all()

        # since the experimenthing ought to be null, don't return it
        unmatched_users = [unmatched_user_tup[0] for unmatched_user_tup in unmatched_users_ret]

        return unmatched_users

    def filter_matchable_users(self, unmatched_users):
        # find just those who have had a status in the last n-days
        valid_unmatched_users = []
        invalid_unmatched_users = []

        random_id_users = [u for u in unmatched_users if
                           u.created_type == utils.common.TwitterUserCreateType.RANDOMLY_GENERATED.value]
        dmca_users = [u for u in unmatched_users if
                      u.created_type == utils.common.TwitterUserCreateType.LUMEN_NOTICE.value]

        # first do random users
        for ru in random_id_users:
            if ru.last_status_dt >= self.period_ago and ru.lang in self.config['match_criteria']['langs']:
                valid_unmatched_users.append(ru)
            else:
                invalid_unmatched_users.append(ru)

        for du in dmca_users:
            if du.lang in self.config['match_criteria']['lang']:
                valid_unmatched_users.append(du)
            else:
                invalid_unmatched_users.append(du)

        return valid_unmatched_users, invalid_unmatched_users

    def invalidate_not_qualifying_users(self, invalid_unmatched_users):
        """
        set the user states on these random-id users to TwitterUserState.not-qualifying so they don't show in queries any more.
        """
        # invalidate by creating a negative-1 randomization arm
        self.insert_ETs(invalid_unmatched_users, randomization_arm=-1, block_id=-1)

    def insert_ETs(self, twitter_users, randomization_arm, block_id):
        ETs_to_add = []
        for tu in twitter_users:
            # query-index in experiment-things to know users are matched
            # object-type # don't use or make the randomization-block id
            # metadata keep randomization-block-id and block sizes other match data here
            metadata = {'block_id': block_id, 'randomization_arm': randomization_arm}
            et = ExperimentThing(query_index=tu.id,
                                 object_type=randomization_arm,
                                 metadata_json=metadata)
            ETs_to_add.append(et)
        self.db_session.add_all(ETs_to_add)
        self.db_session.commit()

    def make_matches(self, valid_unmatched_users):
        """
        this is where matching between comparison and control occur.
        note there's no guarantee that the groups will be the same size so we return matches, still_unmatched tuple
        """
        matches = [valid_unmatched_users]  # list of matches but bypassing for now
        still_unmatched = []
        return matches, still_unmatched

    def save_matches(self, matches):
        """
        Simply persist the matches and their ids to the ET table.
        Note the way we are marking something as matched is that the query-id in ET will be a Twitter ID.
        """
        random_id_users = []
        dmca_users = []
        for match in matches:
            random_id_match = [u for u in match if
                               u.created_type == utils.common.TwitterUserCreateType.RANDOMLY_GENERATED.value]
            random_id_users.extend(random_id_match)
            dmca_match = [u for u in match if
                          u.created_type == utils.common.TwitterUserCreateType.LUMEN_NOTICE.value]
            dmca_users.extend(dmca_match)

        self.insert_ETs(random_id_users,
                        randomization_arm=utils.common.TwitterUserCreateType.RANDOMLY_GENERATED.value,
                        block_id=self.match_id)
        self.insert_ETs(dmca_users,
                        randomization_arm=utils.common.TwitterUserCreateType.LUMEN_NOTICE.value,
                        block_id=self.match_id)

    def report_matching_status(self):
        """
        how many users are in each arm, and in each dategroup.
        :return:
        """
