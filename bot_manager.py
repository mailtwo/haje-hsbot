import os
import io
import json
import sys
import unicodedata
from slackclient import SlackClient
from db_connector import DBConnector


MSG_TYPE = {
    'invalid': -1,
    'user_query': 1,
    'in_channel_msg': 2,
    'insert_alias': 3,
    'user_card_text_query': 4
}

def get_namelist_length(name_list):
    max_str_len = 0
    each_card_len = []
    for name in name_list:
        cur_len = 0
        for c in name:
            if ord(c) < 128:
                cur_len += 1
            else:
                cur_len += 2
        each_card_len.append(cur_len)
        if max_str_len < cur_len:
            max_str_len = cur_len

    return each_card_len, max_str_len

class MsgPair():
    def __init__(self, msg_type, msg_info):
        self.msg_type = msg_type
        if msg_type == 'snippet' and type(msg_info) == tuple:
            self.msg_type = 'snippet_title'
            self.msg_title = msg_info[0]
            self.msg_text = msg_info[1]
        else:
            self.msg_text = msg_info

class BotManager():
    def __init__(self, mode):
        self.mode = mode
        self.db = DBConnector(mode)
        self.db.load(os.path.join('database', 'card_info.pd'), os.path.join('database', 'alias.pd'))

        self.version = 'V1.1.0'
        self.sc = None
        self.channel_id = None
        self.slack_token = None
        with open('help.txt', 'r', encoding='utf-8') as f:
            self.help_message = self._read_help_file(f)


        # user_query = '성기사 전설 천상의 보호막 1코 2코 3코 4코 5코 6코 7코 8코 9코 10코 무기 대 마상시합 고블린 대 노움'
        # stat_query, text_query = self.db.parse_query_text(user_query)
        # print (stat_query, text_query)
        # inner_result = None
        # if len(stat_query.keys()) > 0:
        #     inner_result = self.db.query_stat(stat_query)
        #     print(inner_result.shape[0])
        # card = self.db.query_text(inner_result, text_query)
        # print(card.shape[0])
        # for idx, row in card.iterrows():
        #     print (row)
        # return
        # self.process_bot_instruction({'text': '하스봇! 삭제 1'})

    def _read_help_file(self, fp):
        lines = fp.readlines()
        ret_help = {}
        start_idx = 0
        cur_scope = ['base']
        for cur_idx, line in enumerate(lines):
            if line[:3] == '---' and line[-4:-1] == '---':
                if start_idx != cur_idx - 1:
                    cur_help_dict = ret_help
                    for s in cur_scope:
                        if s not in cur_help_dict:
                            cur_help_dict[s] = {}
                        cur_help_dict = cur_help_dict[s]
                    cur_help_dict['text'] = ''.join(lines[start_idx:cur_idx])

                text_scope = line[3:-4]
                text_scope = text_scope.split('/')
                text_scope = [s.strip() for s in text_scope]
                cur_scope = text_scope
                start_idx = cur_idx + 1

        if start_idx != len(lines)-1:
            cur_help_dict = ret_help
            for s in cur_scope:
                if s not in cur_help_dict:
                    cur_help_dict[s] = {}
                cur_help_dict = cur_help_dict[s]
            cur_help_dict['text'] = ''.join(lines[start_idx:])

        return ret_help

    def get_help_message(self, scope):
        cur_help_dict = self.help_message
        for s in scope:
            if s not in cur_help_dict:
                return None
            cur_help_dict = cur_help_dict[s]
        if 'text' not in cur_help_dict:
            return None

        return '```' + cur_help_dict['text'] + '```'

    def process_card_message(self, card_infos, query_text):
        if len(card_infos) == 1:
            card = card_infos[0]
            stat_text = ''
            if card['type'] == '하수인' or card['type'] == '무기':
                stat_text = '%d코스트 %d/%d' % (card['cost'], card['attack'], card['health'])
            elif card['type'] == '주문' or card['type'] == '영웅 교체':
                stat_text = '%d코스트' % (card['cost'], )
            faction_text = '%s %s %s%s카드' % (card['expansion'], card['hero'], card['rarity'], ' ' if len(card['rarity']) > 0 else '')
            stat_text = '%s %s%s%s' % (stat_text, card['race'], ' ' if len(card['race']) > 1 else '', card['type'])
            card_info = {
                'author_name': faction_text + '\n'+stat_text,
                'footer': '이미지',
                'title': card['orig_name'],
                'color': '#2eb886',
                'title_link': card['detail_url'],
                'image_url': card['img_url'],
            }
            if len(card['card_text']) > 0:
                field_info = [
                    {
                        'title': '효과',
                        'value': card['card_text']
                    }
                ]
                card_info['fields'] = field_info
            self.send_attach_message([card_info])

        elif len(card_infos) <= 5:
            ret_text = []
            for idx in range(len(card_infos)):
                card = card_infos[idx]
                ret_text.append('<%s|%s>' % (card['detail_url'],
                                            '[' + card['orig_name'] + ']'))
            ret_text = ', '.join(ret_text)
            self.send_message(ret_text)

        else:
            ret_text = []
            #ret_text.append('%d 건의 결과가 검색되었습니다.' % (len(card_infos), ))
            each_card_len, max_str_len = get_namelist_length([card['orig_name'] for card in card_infos])

            for idx, card in enumerate(card_infos):
                if card['type'] == '하수인' or card['type'] == '무기':
                    stat_text = '%d코스트 %d/%d' % (card['cost'], card['attack'], card['health'])
                elif card['type'] == '주문' or card['type'] == '영웅 교체':
                    stat_text = '%d코스트' % (card['cost'], )
                stat_text = '%s %s %s %s' % (card['hero'], card['rarity'], card['type'], stat_text)
                cur_text = '%s%s%s' %  (card['orig_name'], ' ' * (max_str_len + 5 - each_card_len[idx]),stat_text)
                ret_text.append(cur_text)

            ret_text = '\n'.join(ret_text)
            self.upload_snippet(ret_text, raw_title='[%s] - 검색결과 %d개' % (query_text, len(card_infos)))

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
            try:
                for msg_info in msg_list:
                    msg_type = self.detect_msg_type(msg_info)
                    if msg_type == MSG_TYPE['user_query']:
                        self.process_user_query(msg_info)
                    elif msg_type == MSG_TYPE['user_card_text_query']:
                        self.process_user_query(msg_info, is_text_for_card_text=True)
                    elif msg_type == MSG_TYPE['in_channel_msg']:
                        self.process_bot_instruction(msg_info)
                    elif msg_type == MSG_TYPE['insert_alias']:
                        msg_pair = self.process_insert_alias(msg_info['text'][2:-2])
                        self.send_msg_pair(msg_pair)
            except Exception as e:
                ret_text = []
                ret_text.append('오류 발생')
                ret_text.append(str(sys.exc_info()[0]))
                ret_text = '\n'.join(ret_text)
                msg_pair = MsgPair('simple_txt', ret_text)
                self.send_msg_pair(msg_pair)

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
            if '=' in text:
                return MSG_TYPE['insert_alias']
            else:
                return MSG_TYPE['user_query']
        elif text[:2] == '((' and text[-2:] == '))':
            return MSG_TYPE['user_card_text_query']
        elif text[:4] == '하스봇!':
            return MSG_TYPE['in_channel_msg']
        return MSG_TYPE['invalid']

    def process_user_query(self, msg_info, is_text_for_card_text=False):
        text = msg_info['text']
        user_query = text[2:-2]
        stat_query, text_query = self.db.parse_query_text(user_query)
        inner_result = None
        if len(stat_query.keys()) > 0:
            inner_result = self.db.query_stat(stat_query)
        if not is_text_for_card_text:
            card = self.db.query_text(inner_result, text_query)
        else:
            card = self.db.query_text_in_card_text(inner_result, text_query)
        if card is not None:
            card_infos = [row for idx, row in card.iterrows()]
        else:
            card_infos = []

        if len(card_infos) == 0:
            ret_text = '%s 에 해당하는 카드를 찾을 수 없습니다.' % (text, )
            self.send_message(ret_text)
        else:
            self.process_card_message(card_infos, user_query)

    def process_bot_instruction(self, msg_info):
        text = msg_info['text']
        instruction = text[4:].strip().lower()
        arg_list = instruction.split(' ')
        if self.mode == 'debug':
            print('인식된 명령어: %s' % (instruction, ))
        if arg_list[0] == '':
            help_message = self.get_help_message(['base'])
            self.send_message(help_message, msg_info['user'])
        elif arg_list[0] == '버전':
            self.send_message('하스봇 버전: %s' %(self.version, ))
        elif arg_list[0] in ['설명', '도움', '도움말', 'help']:
            help_args = ['도움말'] + arg_list[1:]
            help_message = self.get_help_message(help_args)
            if help_message is None:
                self.send_message('해당하는 도움말을 찾을 수 없습니다: ' + str(help_args), msg_info['user'])
            else:
                self.send_message(help_message, msg_info['user'])
        elif arg_list[0] == '등록':
            text = ' '.join(arg_list[1:])
            msg_pair = self.process_insert_alias(text)
            self.send_msg_pair(msg_pair)
        elif arg_list[0] == '리스트':
            text = ' '.join(arg_list[1:])
            msg_pair = self.process_list_alias(text)
            self.send_msg_pair(msg_pair)
        elif arg_list[0] == '삭제':
            text = ' '.join(arg_list[1:])
            msg_pair = self.process_remove_alias(text)
            self.send_msg_pair(msg_pair)
        elif arg_list[0] == '수정':
            text = ' '.join(arg_list[1:])
            msg_pair = self.process_update_alias(arg_list)
            self.send_msg_pair(msg_pair)

    def process_insert_alias(self, text):
        sep_idx = text.find('=')
        if sep_idx < 0:
            return MsgPair('simple_txt', '= 기호를 찾을 수 없습니다.')

        user_query = text[:sep_idx].strip()
        stat_query, text_query = self.db.parse_query_text(user_query)
        inner_result = None
        if len(stat_query.keys()) > 0:
            inner_result = self.db.query_stat(stat_query)

        cards = self.db.query_text(inner_result, text_query)
        card_alias = self.db.normalize_text(text[sep_idx + 1:].strip(), cannot_believe=True)
        if len(card_alias) == 0:
            return MsgPair('simple_txt', '별명을 작성해주세요.')
        if cards.shape[0] == 0:
            return MsgPair('simple_txt', '[%s] 에 해당하는 카드를 찾을 수 없습니다.' % (user_query,))
        elif cards.shape[0] > 1:
            card_to_print = cards.shape[0]
            if card_to_print > 5:
                card_to_print = 5
            ret_text = []
            for idx in range(card_to_print):
                card = cards.iloc[idx]
                ret_text.append('<%s|%s>' % (card['detail_url'],
                                             '[' + card['orig_name'] + ']'))
            ret_text = ', '.join(ret_text) + ('...' if cards.shape[0] > 5 else '')
            ret_text = ('[%s] 에 해당하는 카드가 너무 많습니다. 검색 결과: %d개\n' % (user_query, cards.shape[0])) + ret_text
            return MsgPair('simple_txt', ret_text)
        self.db.insert_alias(cards.iloc[0], card_alias)
        self.db.flush_alias_db()
        return MsgPair('simple_txt', '성공적으로 등록되었습니다.')

    def process_list_alias(self, text):
        user_query = text
        card = None
        if len(user_query) > 0:
            stat_query, text_query = self.db.parse_query_text(user_query)
            inner_result = None
            if len(stat_query.keys()) > 0:
                inner_result = self.db.query_stat(stat_query)
            cards = self.db.query_text(inner_result, text_query)
            if cards.shape[0] == 0:
                return MsgPair('simple_txt', '[%s] 에 해당하는 카드를 찾을 수 없습니다.' % (user_query,))
            elif cards.shape[0] > 1:
                card_to_print = cards.shape[0]
                if card_to_print > 5:
                    card_to_print = 5
                ret_text = []
                for idx in range(card_to_print):
                    card = cards.iloc[idx]
                    ret_text.append('<%s|%s>' % (card['detail_url'],
                                                 '[' + card['orig_name'] + ']'))
                ret_text = ', '.join(ret_text) + ('...' if cards.shape[0] > 5 else '')
                ret_text = ('[%s] 에 해당하는 카드가 너무 많습니다. 검색 결과: %d개\n' % (user_query, cards.shape[0])) + ret_text
                return MsgPair('simple_txt', ret_text)
            card = cards.iloc[0]
        result = self.db.get_alias_list(card)
        if len(result) == 0:
            return MsgPair('simple_txt', '조건에 해당하는 별명이 없습니다.')
        each_name_len, max_str_len = get_namelist_length([info['name'] for info in result])
        ret_line = []
        ret_line.append('아이디%s 이름%s 별명'% (' ' * (10 - 2*3), ' ' * (max_str_len + 5 - 2 * 2)))
        for idx, row in enumerate(result):
            ret_line.append('%d%s %s%s %s' % (row['id'], ' ' * (10 - len(str(row['id']))),
                                             row['name'],' ' * (max_str_len + 5 - each_name_len[idx]),
                                             row['alias']))
        ret_line = '\n'.join(ret_line)
        snippet_title = '별명 검색 결과: %d개' % (len(result), )
        return MsgPair('snippet', (snippet_title, ret_line))

    def process_remove_alias(self, text):
        text = text.strip()
        if not text.isdigit():
            return MsgPair('simple_txt', '별명 아이디를 인식할 수 없습니다.')
        alias_id = int(text)
        result = self.db.delete_alias(alias_id)
        if result == 'empty':
            return MsgPair('simple_txt', '아이디에 해당하는 별명을 찾을 수 없습니다.')
        self.db.flush_alias_db()
        return MsgPair('simple_txt', '성공적으로 삭제되었습니다.')

    def process_update_alias(self, arg_list):
        alias_id = arg_list[1]
        if not alias_id.isdigit():
            return MsgPair('simple_txt', '별명 아이디를 인식할 수 없습니다.')
        alias_id = int(alias_id)

        alias_to = ''.join(arg_list[2:])
        alias_to = self.db.normalize_text(alias_to.strip(), cannot_believe=True)

        result = self.db.update_alis(alias_id, alias_to)
        if result == 'empty':
            return MsgPair('simple_txt', '아이디에 해당하는 별명을 찾을 수 없습니다.')
        self.db.flush_alias_db()
        return MsgPair('simple_txt', '성공적으로 업데이트되었습니다.')

    def send_msg_pair(self, msg_pair):
        if msg_pair is not None:
            if msg_pair.msg_type == 'snippet_title':
                self.upload_snippet(msg_pair.msg_text, raw_title=msg_pair.msg_title)
            elif msg_pair.msg_type == 'snippet':
                self.upload_snippet(msg_pair.msg_text)
            elif msg_pair.msg_type == 'simple_txt':
                self.send_message(msg_pair.msg_text)

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

    def send_attach_message(self, msg_info, channel=None):
        if channel is None:
            channel = self.channel_id
        assert self.sc is not None
        self.sc.api_call(
            'chat.postMessage',
            channel=channel,
            username='하스봇',
            icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
            attachments=msg_info
        )

    def upload_snippet(self, raw_text, raw_title='', channel=None):
        if channel is None:
            channel = self.channel_id
        assert self.sc is not None
        self.sc.api_call(
            'files.upload',
            channels=channel,
            file=io.BytesIO(str.encode(raw_text)),
            filetype='text',
            title=raw_title
        )