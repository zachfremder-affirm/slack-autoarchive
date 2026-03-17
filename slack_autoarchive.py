#!/usr/bin/env python
"""
This program lets you do archive slack channels which are no longer active.
"""

# standard imports
from datetime import datetime, timedelta
import os
import sys
import time
import json
import logging
import requests
import argparse # NEW: Import argparse for command-line arguments

# Helper functions
def get_channel_reaper_settings(workspace_config): # MODIFIED: Takes a workspace config dictionary
    """
    Returns a dictionary of settings for the channel reaper for a given workspace.
    """
    # Define default settings
    default_settings = {
        'dry_run': True, # Default to dry run for safety
        'days_inactive': 90,
        'skip_subtypes': ['channel_join', 'group_join', 'channel_leave', 'group_leave'],
        'min_members': 0,
        'skip_channel_str': '%%noarchive',
        'admin_channel': '',
        'whitelist_keywords': ''
    }

    # Merge default settings with workspace-specific settings,
    # allowing workspace_config to override defaults.
    settings = {**default_settings, **workspace_config}

    # Ensure a Slack token is provided for the workspace
    if 'slack_token' not in settings or not settings['slack_token']:
        raise ValueError(f"Slack token not found for workspace '{settings.get('workspace_name', 'Unnamed')}' in config.json.")

    # Calculate too_old_datetime based on days_inactive
    settings['too_old_datetime'] = datetime.now() - timedelta(days=settings['days_inactive'])
    return settings

def get_logger(name, log_file):
    """
    Returns a logger instance.
    """
    logger = logging.getLogger(name)
    # Clear existing handlers to prevent duplicate logs if logger is re-initialized
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File handler (unique for each workspace to avoid mixing logs)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


class ChannelReaper():
    """
    This class can be used to archive slack channels.
    """

    def __init__(self, settings, workspace_id): # MODIFIED: Accept settings and workspace_id
        self.settings = settings
        # MODIFIED: Use workspace_id in log file name for clarity
        log_file_name = f'./audit_{workspace_id}.log'
        self.logger = get_logger(f'channel_reaper_{workspace_id}', log_file_name)
        self.newly_joined_channels = set()

    def get_whitelist_keywords(self):
        """
        Get all whitelist keywords. If this word is used in the channel
        purpose or topic, this will make the channel exempt from archiving.
        """
        keywords = []
        if os.path.isfile('whitelist.txt'): # Global whitelist file, or make it workspace specific if needed
            with open('whitelist.txt', 'r', encoding='utf-8') as filecontent:
                keywords = filecontent.readlines()

        keywords = list(map(lambda x: x.strip(), keywords))
        whitelist_keywords_from_settings = self.settings.get('whitelist_keywords')
        if whitelist_keywords_from_settings:
            keywords.extend(whitelist_keywords_from_settings.split(','))
        return list(keywords)

    def get_channel_alerts(self):
        """Get the alert message which is used to notify users in a channel of archival. """
        archive_msg = """
This channel has had no activity for {} days. It is being auto-archived.
If you feel this is a mistake you can <https://get.slack.help/hc/en-us/articles/201563847-Archive-a-channel#unarchive-a-channel|unarchive this channel>.
This will bring it back at any point. In the future, you can add '%%noarchive' to your channel topic or purpose to avoid being archived.
If you have any questions please reach out to Affirm IT. 
"""

        if os.path.isfile('templates.json'): # Global template file, or make it workspace specific
            with open('templates.json', 'r', encoding='utf-8') as filecontent:
                archive_msg = json.load(filecontent)['channel_template']

        archive_msg = archive_msg.format(self.settings.get('days_inactive'))
        return {'channel_template': archive_msg}

    # pylint: disable=too-many-arguments
    def slack_api_http(
            self,
            api_endpoint=None,
            payload=None,
            method='GET',
            retry=True,
            retry_delay=0):
        """ Helper function to query the slack api and handle errors and rate limit. """
        uri = 'https://slack.com/api/' + api_endpoint
        headers = {'Authorization': 'Bearer ' + self.settings.get('slack_token')}
        try:
            if retry_delay > 0:
                time.sleep(retry_delay)

            if method == 'POST':
                response = requests.post(uri, json=payload, headers=headers)
            else:
                response = requests.get(uri, params=payload, headers=headers)

            if response.status_code == requests.codes.ok and 'error' in response.json():
                error_code = response.json()['error']
                if error_code == 'not_authed':
                    self.logger.error(
                        'Authentication failed. Check your Slack token for this workspace.'
                    )
                    sys.exit(1)
                elif error_code == 'not_in_channel':
                    self.logger.error(
                        f"Bot not in channel {payload.get('channel')}. Attempting to join..."
                    )
                    # Attempt to join the channel
                    if self.join_channel(payload['channel']): # Check if join was successful
                        # After joining, retry the original request (e.g., conversations.history)
                        self.logger.info(f"Retrying API call {api_endpoint} after joining channel.")
                        return self.slack_api_http(api_endpoint, payload, method, retry, 1) # Add a small delay after join
                    else:
                        self.logger.error(f"Failed to join channel {payload.get('channel')}, cannot complete {api_endpoint} call.")
                        return None
                else:
                    self.logger.error(
                        f"Slack API error: {error_code} for endpoint {api_endpoint} with payload {payload}"
                    )
                    return None
            elif response.status_code == requests.codes.ok and response.json()['ok']:
                return response.json()
            elif response.status_code == requests.codes.too_many_requests:
                retry_timeout = float(response.headers['Retry-After'])
                self.logger.warning(f"Rate limit hit. Retrying after {retry_timeout} seconds for {api_endpoint}.")
                return self.slack_api_http(api_endpoint, payload, method, False, retry_timeout)
            else: # Handle other HTTP errors that are not 200 OK
                self.logger.error(
                    f"HTTP Error {response.status_code} for {api_endpoint}: {response.text}"
                )
                return None
        except requests.exceptions.RequestException as e: # Catch network/connection errors
            self.logger.error(f"Network/Connection error during API call to {api_endpoint}: {e}")
            return None
        except Exception as error_msg:
            self.logger.error(f"An unexpected error occurred during API call to {api_endpoint}: {error_msg}")
            return None
        return None

    def join_channel(self, channel_id):
        api_endpoint = 'conversations.join'
        info_payload = {'channel': channel_id}
        # Do not retry join_channel itself here to avoid infinite loops if join constantly fails
        join_response = self.slack_api_http(api_endpoint=api_endpoint,
                                            payload=info_payload,
                                            method='POST',
                                            retry=False)
        if join_response and join_response.get('ok'):
            self.logger.info(f'Joined channel {channel_id}')
            self.newly_joined_channels.add(channel_id)
            return True
        else:
            # Error logging already happens in slack_api_http, so no need for duplicate
            return False

    def leave_channel(self, channel_id):
        api_endpoint = 'conversations.leave'
        payload = {'channel': channel_id}
        leave_response = self.slack_api_http(api_endpoint=api_endpoint,
                                             payload=payload,
                                             method='POST',
                                             retry=False)
        if leave_response and leave_response.get('ok'):
            self.logger.info(f"Left channel {channel_id}")
            self.newly_joined_channels.discard(channel_id)
        # Error logging for leave_channel is handled by slack_api_http

    def get_all_channels(self):
        """ Get a list of all non-archived channels from slack channels.list. """
        payload = {'exclude_archived':1, 'limit':100}
        api_endpoint = 'conversations.list'

        flag_first_page = True
        flag_last_page = False
        next_cursor = ''
        all_channels = []
        while not flag_last_page:
            if not flag_first_page:
                payload['cursor'] = next_cursor
            api_response = self.slack_api_http(api_endpoint=api_endpoint, payload=payload)

            if not api_response:
                self.logger.error("Failed to retrieve channels from Slack API.")
                break

            channels = api_response.get('channels', [])
            self.logger.info('%s channel(s) retrieved from this page.' % str(len(channels)))
            for channel in channels:
                all_channels.append({
                    'id': channel['id'],
                    'name': channel['name'],
                    'created': channel['created'],
                    'num_members': channel.get('num_members', 0)
                })

            next_cursor = api_response.get('response_metadata', {}).get('next_cursor')
            if not next_cursor:
                flag_last_page = True

            flag_first_page = False # Ensure this is set after the first page fetch


        return all_channels

    def get_last_message_timestamp(self, channel_history, channel_created_datetime): # Renamed too_old_datetime to channel_created_datetime for clarity
        """ Get the last message from a slack channel, and return the time. """
        # Default to creation time if no messages found
        last_message_datetime = channel_created_datetime
        is_user_message_found = False

        if 'messages' not in channel_history or not channel_history['messages']:
            return (last_message_datetime, False) # No messages found, so use creation time and no user activity

        # Find the latest message that is NOT a skipped subtype
        for message in channel_history['messages']:
            if 'subtype' in message and message['subtype'] in self.settings.get('skip_subtypes', []):
                continue
            last_message_datetime = datetime.fromtimestamp(float(message['ts']))
            # Determine if it's a user message (no bot_id or app_id)
            if 'bot_id' not in message and 'app_id' not in message:
                is_user_message_found = True
            break # Found the latest relevant message, no need to continue

        return (last_message_datetime, is_user_message_found)

    def is_channel_disused(self, channel, too_old_datetime):
        """ Return True or False depending on if a channel is "active" or not.  """
        num_members = channel.get('num_members', 0)
        payload = {'inclusive': 0, 'oldest': 0, 'limit': 50} # Fetch enough messages to be confident in last activity
        api_endpoint = 'conversations.history'

        payload['channel'] = channel['id']
        channel_history = self.slack_api_http(api_endpoint=api_endpoint,
                                              payload=payload)

        if not channel_history:
            self.logger.warning(f"Could not retrieve history for channel {channel['name']}. Assuming not disused for safety.")
            return False

        # Pass channel creation time as a fallback if no messages exist
        channel_created_datetime = datetime.fromtimestamp(float(channel['created']))
        (last_message_datetime, is_user) = self.get_last_message_timestamp(
            channel_history, channel_created_datetime)

        min_members = self.settings.get('min_members', 0)
        # A channel has "enough users" if num_members is greater than min_members.
        # If min_members is 0, this condition is always true (no minimum).
        # has_enough_users is True if num_members > min_members
        has_enough_users = (num_members > min_members)

        # A channel is disused if:
        # 1. The last relevant message is older than 'too_old_datetime'.
        # AND
        # 2. (It was a bot message OR the channel does NOT have enough users).
        # This logic ensures that if there are enough users, we primarily care about *user* activity.
        # If there are not enough users, even bot activity might not save it.
        return last_message_datetime <= too_old_datetime and (not is_user or not has_enough_users)

    def is_channel_whitelisted(self, channel, white_listed_keywords):
        """ Return True or False depending on if a channel is exempt from being archived. """
        info_payload = {'channel': channel['id']}
        channel_info_response = self.slack_api_http(api_endpoint='conversations.info',
                                                     payload=info_payload,
                                                     method='GET')

        if not channel_info_response or not channel_info_response.get('ok'):
            self.logger.warning(f"Could not retrieve info for channel {channel['name']}. Cannot determine whitelist status. Assuming NOT whitelisted.")
            return False # Default to not whitelisted if info cannot be retrieved

        channel_info = channel_info_response.get('channel', {})
        channel_purpose = channel_info.get('purpose', {}).get('value', '')
        channel_topic = channel_info.get('topic', {}).get('value', '')

        skip_str = self.settings.get('skip_channel_str')
        if skip_str and (skip_str in channel_purpose or skip_str in channel_topic):
            self.logger.info(f"Channel #{channel['name']} whitelisted by skip string in topic/purpose ('{skip_str}').")
            return True

        for white_listed_keyword in white_listed_keywords:
            # Check case-insensitively
            if white_listed_keyword.lower() in channel['name'].lower() or \
               white_listed_keyword.lower() in channel_purpose.lower() or \
               white_listed_keyword.lower() in channel_topic.lower():
                self.logger.info(f"Channel #{channel['name']} whitelisted by keyword: '{white_listed_keyword}'.")
                return True
        return False

    def send_channel_message(self, channel_id, message):
        """ Send a message to a channel or user. """
        payload = {
            'channel': channel_id,
            'text': message
        }
        api_endpoint = 'chat.postMessage'
        response = self.slack_api_http(api_endpoint=api_endpoint,
                                       payload=payload,
                                       method='POST')
        if response and response.get('ok'):
            self.logger.info(f"Message sent to channel {channel_id}.")
        else:
            self.logger.error(f"Failed to send message to channel {channel_id}.")

    def archive_channel(self, channel, channel_message):
        """ Archive a channel, and send alert to slack admins. """
        api_endpoint = 'conversations.archive'
        stdout_message = f"Attempting to archive channel: #{channel['name']} (ID: {channel['id']})"
        self.logger.info(stdout_message)

        if not self.settings.get('dry_run'):
            # Send alert message to the channel first
            self.send_channel_message(channel['id'], channel_message)
            time.sleep(1) # Give a small delay before archiving

            payload = {'channel': channel['id']}
            archive_response = self.slack_api_http(api_endpoint=api_endpoint, payload=payload, method='POST')

            if archive_response and archive_response.get('ok'):
                self.logger.info(f"Successfully archived channel: #{channel['name']}")
                # If archived, remove from newly_joined_channels as it won't be left
                self.newly_joined_channels.discard(channel['id'])
            else:
                self.logger.error(f"Failed to archive channel: #{channel['name']}.")
        else:
            self.logger.info(f"[DRY RUN] Would have archived channel: #{channel['name']}")

    def send_admin_report(self, channels):
        """ Optionally this will message admins with which channels were archived. """
        admin_channel_id = self.settings.get('admin_channel')
        if admin_channel_id and channels:
            channel_names = ', '.join('#' + channel['name']
                                      for channel in channels)
            admin_msg = 'Archiving %d channels: %s' % (len(channels),
                                                        channel_names)
            if self.settings.get('dry_run'):
                admin_msg = '[DRY RUN] %s' % admin_msg
            self.send_channel_message(admin_channel_id, admin_msg)
        elif admin_channel_id and not channels:
            self.logger.info("No channels were archived (or would have been in dry run). No admin report sent.")
        else:
            self.logger.info("Admin channel not configured. Skipping admin report.")

    def main(self):
        """
        This is the main method that checks all inactive channels and archives them.
        """
        self.logger.info(f"--- Starting Channel Reaper for Workspace: {self.settings.get('workspace_name', 'Unnamed')} ---")
        if self.settings.get('dry_run'):
            self.logger.info(
                'THIS IS A DRY RUN. NO CHANNELS ARE ACTUALLY ARCHIVED.')

        whitelist_keywords = self.get_whitelist_keywords()
        alert_templates = self.get_channel_alerts()
        archived_channels = []

        all_slack_channels = self.get_all_channels()
        if not all_slack_channels:
            self.logger.error("No channels retrieved. Exiting processing for this workspace.")
            return

        self.logger.info(f"\n--- Detailed Channel Report ({len(all_slack_channels)} channels) ---")
        for channel in all_slack_channels:
            if channel['id'].startswith('D') or channel['id'].startswith('G'):
                self.logger.debug(f"Skipping detailed report for DM/MPDM channel: {channel['name']}")
                continue

            history_payload = {'inclusive': 0, 'oldest': 0, 'limit': 1}
            history_payload['channel'] = channel['id']
            channel_history = self.slack_api_http(api_endpoint='conversations.history',
                                                  payload=history_payload)

            last_message_date_str = "N/A"
            if channel_history and channel_history.get('messages'):
                latest_message_ts = channel_history['messages'][0]['ts']
                last_message_datetime = datetime.fromtimestamp(float(latest_message_ts))
                last_message_date_str = last_message_datetime.strftime('%Y-%m-%d %H:%M:%S')
            elif channel_history and not channel_history.get('messages'):
                last_message_date_str = "No messages found"
            else:
                last_message_date_str = "Failed to retrieve history"


            self.logger.info(
                f"Channel: #{channel['name']} "
                f"(ID: {channel['id']}, "
                f"Members: {channel.get('num_members', 'N/A')}, "
                f"Last Message: {last_message_date_str})"
            )
        self.logger.info("-------------------------------------------------")


        self.logger.info(f"Processing {len(all_slack_channels)} channels...")

        for channel in all_slack_channels:
            sys.stdout.write('.')
            sys.stdout.flush()

            if channel['id'].startswith('D') or channel['id'].startswith('G'):
                 self.logger.debug(f"Skipping DM/MPDM channel: {channel['name']}")
                 continue

            channel_whitelisted = self.is_channel_whitelisted(
                channel, whitelist_keywords)
            channel_disused = self.is_channel_disused(
                channel, self.settings.get('too_old_datetime'))

            if (not channel_whitelisted and channel_disused):
                self.logger.info(f"Channel #{channel['name']} is disused and not whitelisted. Marking for archival.")
                archived_channels.append(channel)
                self.archive_channel(channel,
                                     alert_templates['channel_template'])
            else:
                self.logger.info(f"Channel #{channel['name']} is active or whitelisted. Skipping archival.")
                if channel['id'] in self.newly_joined_channels and not self.settings.get('dry_run'):
                    self.logger.info(f"Bot newly joined and not archiving channel #{channel['name']}. Attempting to leave.")
                    self.leave_channel(channel['id'])
                elif channel['id'] in self.newly_joined_channels and self.settings.get('dry_run'):
                    self.logger.info(f"[DRY RUN] Bot newly joined and not archiving channel #{channel['name']}. Would have left.")


        self.logger.info("\nFinished processing channels for this workspace.")
        self.send_admin_report(archived_channels)


if __name__ == '__main__':
    # NEW: Command-line argument parsing
    parser = argparse.ArgumentParser(description="Archive inactive Slack channels across multiple workspaces.")
    parser.add_argument(
        '--workspace',
        type=str,
        help="Specify a Slack workspace ID from config.json to run for. If omitted, runs for ALL workspaces."
    )
    args = parser.parse_args()

    # Load all workspace configurations from config.json
    all_workspace_configs = {}
    if os.path.isfile('config.json'):
        try:
            with open('config.json', 'r', encoding='utf-8') as f:
                all_workspace_configs = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error reading config.json: {e}")
            sys.exit(1)
    else:
        print("Error: 'config.json' not found. Please create it with your workspace configurations.")
        sys.exit(1)

    if args.workspace:
        # Run for a specific workspace
        workspace_id_to_run = args.workspace
        if workspace_id_to_run not in all_workspace_configs:
            print(f"Error: Workspace ID '{workspace_id_to_run}' not found in 'config.json'.")
            sys.exit(1)

        print(f"\n--- Running for specified workspace: {workspace_id_to_run} ---")
        try:
            workspace_specific_settings = all_workspace_configs[workspace_id_to_run]
            # Add workspace_name to settings for logging clarity
            workspace_specific_settings['workspace_name'] = workspace_id_to_run
            
            settings = get_channel_reaper_settings(workspace_specific_settings)
            channel_reaper_instance = ChannelReaper(settings, workspace_id_to_run) # Pass workspace_id to ChannelReaper
            channel_reaper_instance.main()
        except ValueError as e:
            print(f"Configuration error for workspace '{workspace_id_to_run}': {e}")
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred for workspace '{workspace_id_to_run}': {e}")
            sys.exit(1)
        print(f"--- Finished for specified workspace: {workspace_id_to_run} ---\n")
    else:
        # Run for all workspaces defined in config.json
        if not all_workspace_configs:
            print("No workspaces defined in 'config.json'. Nothing to do.")
            sys.exit(0)

        for ws_id, ws_config in all_workspace_configs.items():
            print(f"\n--- Running for workspace: {ws_id} ---")
            try:
                # Add workspace_name to settings for logging clarity
                ws_config['workspace_name'] = ws_id

                settings = get_channel_reaper_settings(ws_config)
                channel_reaper_instance = ChannelReaper(settings, ws_id) # Pass workspace_id to ChannelReaper
                channel_reaper_instance.main()
            except ValueError as e:
                print(f"Configuration error running for workspace '{ws_id}': {e}")
            except Exception as e:
                print(f"An unexpected error occurred for workspace '{ws_id}': {e}")
            print(f"--- Finished for workspace: {ws_id} ---\n")