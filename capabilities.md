# CivilServant System Capabilities Summary

Eric Pennington
(*Last Updated Apr 28, 2023*)

This document contains descriptions of the capabilities of the
CivilServant system along with required permissions from the target
platform, including:

-   [Units of Assignment](#Units-of-Assignment)
-   [Platform Interventions](#Platform-Interventions)
-   [Outcome Variables](#Outcome-Variables)
-   [Required Target Platform Permissions](#Required-Target-Platform-Permissions)


## Units of Assignment

Most objects (users, posts, etc) available through a target platform's
API can be combined with additional criteria for use as units of
assignment. Additional units of assignment can be added as needed. These
include:

### Reddit

-   **Individual Users**
    -   Newcomers: Made at least one comment within a subreddit in the
            last X days.
    -   Users responding to a particular post.
    -   Users having certain flair assigned.
    -   Users that have had particular moderator actions taken on their
            account (such as banning, having a comment deleted, etc).

-   **Posts**
    -   A post stickied in the subreddit.
    -   A post that made the front page of Reddit.
    -   A post containing or not containing particular words.
    -   A post with particular moderator actions taken on it.

-   **Comments**
    -   A comment stickied to a post.
    -   A reply to another sticked comment.
    -   A comment containing or not containing particular words.
    -   A comment with particular moderator actions taken on it.

### Wikipedia

-   **Individual Users**
    -   New Editors: Users that have signed up within a particular
            period and only have a level of activity that doesn't
            cross a specified threshold.
    -   Editors who have sent thanks to other users.
    -   Editors who have received thanks from other users.
    -   Editors with empty discussion pages.

-   **Individual Articles**

-   **Individual Edits**
    -   Edits made by a new editor.
    -   Edits made by a senior editor.

## Platform Interventions

These are the interventions the system can perform on a target platform.
Additional interventions can be added easily so long as the platform's
API allows it and the CivilServant bot has the permissions needed.

### Reddit

-   Sending a private message to a user.
-   Making a post.
-   Making a comment.
-   Stickying a post.
-   Stickying a comment.
-   Performing a moderation action like removing a comment.

**Specific Examples:**

-   Sticky a comment on each new submission in a subreddit with rules
        for how potential commenters should interact with it.
-   Remove non-question comments from a submission (i.e. comments that
        don't contain a question mark).

### Wikipedia

-   Editing an article.
-   Posting on a user's discussion page.
-   Posting on an article's discussion page.
-   Sending thanks to a specific user for a specific edit.

**Specific Examples:**

-   Post a message to newcomer editors discussion pages welcoming them
        to that language Wikipedia and some notes to get them started.
-   Send a thanks to newcomer editors who made a recent edit that passes
        muster.

## Outcome Variables

These are the bulk of the outcome variables the system is able to
measure, organized by platform and unit of analysis level (user, post,
comment, etc).

### Reddit

**Comment**
-   **Newcomer Comment Rule Compliance:** Whether a newcomer\'s first
        comment remained or got removed by a moderator.
-   **Newcomer First Comment Score:** Score of the first comment made by
        a newcomer.
-   **Allowed:** Whether a comment made by a previously banned user was
        allowed to remain when posted after the banning period concluded.
-   **Link Included:** Whether a link was included in a comment.

**Post**
-   **\# of Comments:** Number of comments per post.
-   **\# of Top-Level Non-Question Comments:** Number of comments
        directly on a post (not comment reply) that do not contain a
        question mark.
-   **\# of Guestbook Comment Replies:** Number of comments made in
        reply to a guestbook comment (a stickied comment on a post).
-   **\# of Newcomer Comments:** Number of newcomer comments per post.
-   **\# of Newcomer Comments Removed:** Number of newcomer comments
        removed per post.
-   **Allowed:** Whether a post made by a previously banned user was
        allowed to remain when posted after the banning period concluded.
-   **Max Rank:** Highest rank achieved by a post.
-   **Highest Rank:** Most prominent rank achieved by a post.
-   **% of Comments Reported:** The percentage of comments on a post
        reported to a subreddit's moderators.
-   **% of Replies:** Percentage of comments on a post that are replies
        to other comments rather than top-level comments on the post.
-   **Sum of Comment Scores:** The sum of the scores of all comments on
        a post.
-   **Mean Comment Score:** The average of the scores of all comments on
        a post.
-   **\# of Reports Received on Comments:** The sum of the number of
        reports made to moderators on the comments of a post.
-   **Political Partisanship:** A measure of the political partisanship
        of commenters on a post, based on their prior participation in
        politically-leaning subreddits.

**Post (per a time period)**
-   **Score:** The score of a post at a moment in time.
-   **Rank:** The current rank of a post within the top 200 "HOT" items
        at a moment in time.
-   **Commenting Rate:** The number of comments per minute for a given
        post.

**User**
-   **\# of Newcomer Comments Reported:** Number of comments by a
        newcomer reported to a subreddit's moderators.
-   **Mean Reports for a Newcomer Comment:** The total number of
        received reports for a newcomer divided by the the total number of
        comments
-   **Participated Without Reban:** A previously banned account made at
        least one new contribution without being rebanned over n-weeks
        after the original banning period.
-   **Participated After Ban:** A previously banned account made at
        least one new contribution over n-weeks after the original banning
        period (regardless of what happened to the account later).
-   **Rebanned:** A previously banned account was permanently rebanned
        within n weeks after the original banning period.

**Experiment (per day)**
-   **\# of Comments Received Per Day:** The number of comments the
        system observed for the experiment on a given day.
-   **% of Comments Removed Per Day:** The percentage of comments the
        system removed for an experiment on a given day.
-   **Mean Comment Score Per Day:** The mean score of comments observed
        by the system for an experiment on a given day.
-   **% of Comments Reported Per Day:** The percentage of comments
        reported to a subreddit's moderators observed by the system for an
        experiment on a given day.
-   **\# Comments Per Minute Per Day:** The number of comments on a post
        included in the study per minute for an experiment on a given day.

### Wikipedia

**User**
-   **Survey Invitation:** Was a user invited to take a survey.
-   **Efficacy:** How strongly a user believes they can make
        contributions that follow Wikipedia\'s expected practices (scale
        of 1 to 5).
-   **Reported User Friendliness:** Reported Community Friendliness: How
        friendly a surveyed user believes a community to be (scale of 1 to
        5).
-   **Competition Participation:** Whether a participant made a
        contribution to a competition (such as WikiLovesAfrica).
-   **\# of Photos Contributed:** Number of photos contributed to a
        contest (such as WikiLovesAfrica).
-   **Positive Feelings:** Measure of change in positive feelings
        between receiving pre- and post- surveys.

**User (per a time period)**
-   **N-Day Activation:** Whether a user made at least one edit in an N
        day period after registration.
-   **N-Week Retention:** Whether an account was active in the N-th week
        after receiving an intervention.
-   **\# of Labor Hours:** Number of hours when edits are made after an
        intervention.
-   **\# of Thanks Sent:** Number of thanks a user sent in a particular
        time period.
-   **Difference in Daily Labor Hours:** Change in labor hours for a
        user within a time period after assignment.
-   **Supportive Actions:** Measure of supportive behavior observed,
        such as sending thanks, observed on a user within a time period.

## Required Target Platform Permissions

Access to some features of a target platform, both for collecting data
and for interventions we perform, require special permissions to be
granted to our bot accounts by the moderators of a particular subreddit
or the admins of a particular language wikipedia respectively.

A non-exhaustive set of permission requests we may or may not need to
make depending on the study are listed below:

### Reddit

-   Stickying a post in a subreddit.
-   Stickying a comment to a post.
-   Access to moderation logs.
-   Performing a moderation action (removing a comment, etc).
-   Access to moderation mail.
-   For private subreddits, access to the subreddit.
-   For restricted subreddits, posting a submission.
-   For restricted subreddits, posting a comment.

### Wikipedia

-   Editing an article on a specific language wikipedia.
-   Posting on a user's discussion page.
-   Posting on an article's discussion page.
-   Using banner ads to recruit users into a study.
-   Most of the more esoteric interactions require specific permissions
        as well, such as:
    -   sending a thanks to a specific user for a specific edit.
    -   creating a barnstar to give to users as a reward.
    -   etc.

For the sake of being good citizens in a community, we will occasionally
ask for permission on things that are not technically required by the
platform as well. For instance, sending direct messages to users of a
particular subreddit, etc.
