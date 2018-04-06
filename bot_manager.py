import os
import json
from slackclient import SlackClient
from db_connector import DBConnector

MSG_TYPE = {
    'invalid': -1,
    'user_query': 1,
    'in_channel_msg': 2
}

class BotManager():
    def __init__(self, mode):
        self.mode = mode
        self.db = DBConnector(mode)
        self.db.load(os.path.join('database', 'card_info.pd'), os.path.join('database', 'alias.pd'))
        self.sc = None
        self.channel_id = None
        self.slack_token = None
        with open('help.txt', 'r', encoding='utf-8') as f:
            self.help_message = f.read()

        # user_query = '코볼트 죽메'
        # stat_query, text_query = db.parse_query_text(user_query)
        # print (stat_query, text_query)
        # inner_result = None
        # if len(stat_query.keys()) > 0:
        #     inner_result = db.query_stat(stat_query)
        #     print(inner_result.shape[0])
        # card = db.query_text(inner_result, text_query)
        # print(card.shape[0])
        # for idx, row in card.iterrows():
        #     print (row)
        # return

    def get_help_message(self):
        return '```' + self.help_message + '```'

    def load_bot_token(self, path):
        if self.mode == 'debug':
            target_channel_name = 'bottest'
        else:
            target_channel_name = 'game_hs'

        if not os.path.exists(path):
            print('Cannot find bot_token json in path %s' % (path, ))
            return False

        with open(path, 'r') as f:
            token_data = json.load(f)
            token_id = token_data['token_id']
            channel_id = token_data['channels'][target_channel_name]

        if self.mode == 'debug':
            print('Bot token: %s' % (str(token_id), ))
            print('Channel: %s (%s)' % (target_channel_name, str(channel_id)))

        self.channel_id = channel_id
        self.slack_token = token_id
        return True
    
    def connect(self):
        assert self.slack_token is not None
        if self.sc is None:
            self.sc = SlackClient(self.slack_token)
            
        if not self.sc.rtm_connect():
            print('Error while sc.rtm_connect()')
            return False

        self.sc.server.websocket.sock.setblocking(1)
        print('Start running...')
        return True

    def run(self):
        while self.sc.server.connected is True:
            msg_list = self.sc.rtm_read()
            for msg_info in msg_list:
                msg_type = self.detect_msg_type(msg_info)
                if msg_type == MSG_TYPE['user_query']:
                    self.process_user_query(msg_info)
                elif msg_type == MSG_TYPE['in_channel_msg']:
                    self.process_bot_instruction(msg_info)

    def detect_msg_type(self, msg_info):
        if msg_info['type'] != 'message':
            return MSG_TYPE['invalid']
        if 'user' not in msg_info or msg_info['user'][0] != 'U':
            return MSG_TYPE['invalid']
        if msg_info['channel'] != self.channel_id:
            if msg_info['channel'][:2] == 'DA':
                text = msg_info['text']
                if text[:4] == '하스봇!':
                    return MSG_TYPE['in_channel_msg']
            return MSG_TYPE['invalid']
        text = msg_info['text']
        if text[:2] == '[[' and text[-2:] == ']]':
            return MSG_TYPE['user_query']
        elif text[:4] == '하스봇!':
            return MSG_TYPE['in_channel_msg']
        return MSG_TYPE['invalid']

    def process_user_query(self, msg_info):
        text = msg_info['text']
        user_query = text[2:-2]
        stat_query, text_query = self.db.parse_query_text(user_query)
        inner_result = None
        if len(stat_query.keys()) > 0:
            inner_result = self.db.query_stat(stat_query)
        card = self.db.query_text(inner_result, text_query)

        ret_text = ''
        if card.empty:
            ret_text = '%s 에 해당하는 카드를 찾을 수 없습니다.' % (text, )
        elif card.shape[0] == 1:
            cur_data = card.iloc[0]
            ret_text = '<%s|%s>\n%s'% (cur_data['detail_url'], '[' + cur_data['orig_name'] + ']', cur_data['img_url'])
        elif card.shape[0] <= 5:
            ret_text = []
            for idx in range(card.shape[0]):
                ret_text.append('<%s|%s>' % (card.iloc[idx]['detail_url'],
                                            '[' + card.iloc[idx]['orig_name'] + ']'))
            ret_text = ', '.join(ret_text)
        else:
            ret_text = []
            for idx in range(5):
                ret_text.append('<%s|%s>' % (card.iloc[idx]['detail_url'],
                                            '[' + card.iloc[idx]['orig_name'] + ']'))
            ret_text = ', '.join(ret_text)
            ret_text = ('%d 건의 결과가 검색되었습니다.\n' % (card.shape[0], )) + ret_text + ' ...'

        self.send_message(ret_text)

    def process_bot_instruction(self, msg_info):
        text = msg_info['text']
        instruction = text[4:].strip().lower()
        if self.mode == 'debug':
            print('인식된 명령어: %s' % (instruction, ))
        if instruction == '버전':
            self.send_message('하스봇 버전: V 0.2.0, 3a5b1112')
        elif instruction in ['설명', '도움', '도움말', 'help']:
            help_message = self.get_help_message()
            self.send_message(help_message, msg_info['user'])
        elif instruction == '등록':
            self.send_message('등록 구현 중')
    
    def send_message(self, msg_text, channel=None, user=None):
        if channel is None:
            channel = self.channel_id
        assert self.sc is not None
        if user is not None:
            self.sc.api_call(
                'chat.postMessage',
                channel=channel,
                username='하스봇',
                icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
                user=user,
                text=msg_text
            )
        else:
            self.sc.api_call(
                'chat.postMessage',
                channel=channel,
                username='하스봇',
                icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
                text=msg_text
            )