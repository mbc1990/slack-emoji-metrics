import time
import sys
import os
import json
import sqlite3
from slackclient import SlackClient
from slacker import Slacker
from multiprocessing import Process

class EmojiCount():
    
    def __init__(self, token):
        
        self.c = self._init_db()
        # self._init_schema()

        # RESTful API 
        self.slack_REST = Slacker(token)

        # Real time API
        self.slack_real_time = SlackClient(token)

        # Read delay in seconds
        self.read_delay = 1

        self.user_name_map = {}
        self.channel_name_map = {}
        self.update_name_maps()


    def _init_db(self):
        """
        Returns a cursor
        """
        conn = sqlite3.connect('emoji.db')
        return conn.cursor()
        

    def _init_schema(self):
        """
        Creates necessary tables for storing data
        """
        query = '''
                CREATE TABLE EMOJI(
                   ID             INT     NOT NULL,
                   USER_ID        TEXT    NOT NULL,
                   EMOJI_ID       TEXT    PRIMARY KEY NOT NULL,
                   CHANNEL_ID     TEXT    NOT NULL,
                   IS_REACTION    INT     NOT NULL,
                   TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                '''
        self.c.execute(query)
    
    def _write_row(self, user_id, emoji_id, channel_id, is_reaction):
        query = '''

                INSERT INTO EMOJI (
                    USER_ID,
                    EMOJI_ID,
                    CHANNEL_ID,
                    IS_REACTTION
                ) 
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                );
                ''' % (user_id, emoji_id, channel_id, is_reaction)
        self.c.execute(query)

    def read_slack(self):
        if self.slack_real_time.rtm_connect():
            while True:
                events = self.slack_real_time.rtm_read()
                for ev in events:
                    print ev
                    if 'type' in ev:
                        if ev['type'] == 'reaction_added':
                            user_id = ev['user']
                            emoji_id = ev['reaction']
                            channel_id = ev['item']['channel']
                            is_reaction = True
                            self._write_row(user_id, emoji_id, channel_id, is_reaction)
                        elif ev['type'] == 'message':
                            text = ev['text']
                            pass
                        else:
                            continue

                    else:
                        continue
                


                time.sleep(self.read_delay)
        else:
            print "Failed to connect"


    def update_name_maps(self):
        """
        Updates the user and channel name maps
        """
        response = self.slack_REST.users.list()
        users = response.body['members']
        for user in users:
            self.user_name_map[user['id']] = user['name']

        response = self.slack_REST.channels.list()
        channels = response.body['channels']
        for channel in channels:
            self.channel_name_map[channel['id']] = channel['name']
        

def main():
    with open('conf.json') as f:    
        conf = json.loads(f.read())
        ec = EmojiCount(conf['SLACK_API_TOKEN'])
        ec.read_slack()


if __name__ == "__main__":
    main()
