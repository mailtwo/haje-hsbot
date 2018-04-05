import os
import sys
import json
from slackclient import SlackClient
from db_connector import DBConnector

card_db_col = ['inven_index', 'name', 'eng_name', 'hero', 'type', 'cost', 'attack', 'health', 'rarity', 'expansion', 'img_url', 'detail_url']

def main():
    mode = 'release'
    if len(sys.argv) == 2:
        mode = sys.argv[1].lower()
        if mode != 'release' and mode != 'debug':
            print('Invalid mode %s: should be \'debug\' or \'release\'' % (mode, ))
            return
    if mode == 'debug':
        target_channel_name = 'bottest'
    else:
        target_channel_name = 'game_hs'
    print('Mode: %s'% (mode, ))

    with open('bot_token.json', 'r') as f:
        token_data = json.load(f)
        token_id = token_data['token_id']
        channel_id = token_data['channels'][target_channel_name]
    print('Bot token: %s' % (str(token_id), ))
    print('Channel: %s (%s)' % (target_channel_name, str(channel_id)))
    slack_token = token_id

    db = DBConnector()
    db.load(os.path.join('database', 'card_info.pd'), os.path.join('database', 'alias.pd'))
    # user_query = '3코 천상의 보호막 도발'
    # stat_query, text_query = db.parse_query_text(user_query)
    # print (stat_query, text_query)
    # inner_result = None
    # if len(stat_query.keys()) > 0:
    #     inner_result = db.query_stat(stat_query)
    #     print(inner_result.shape[0])
    # card = db.query_text(inner_result, text_query)
    # print(card.shape[0])
    # for idx, row in card.iterrows():
    #     print (row['name'])

    sc = SlackClient(slack_token)

    if sc.rtm_connect():
        sc.server.websocket.sock.setblocking(1)
        print ('Start running...')
        while sc.server.connected is True:
            msg_list = sc.rtm_read()
            for msg_info in msg_list:
                if msg_info['type'] != 'message':
                    continue
                if msg_info['channel'] != channel_id:
                    continue
                if 'user' not in msg_info or msg_info['user'][0] != 'U':
                    continue
                text = msg_info['text']

                if not(text[:2] == '[[' and text[-2:] == ']]'):
                    continue

                user_query = text[2:-2]
                stat_query, text_query = db.parse_query_text(user_query)
                inner_result = None
                if len(stat_query.keys()) > 0:
                    inner_result = db.query_stat(stat_query)
                card = db.query_text(inner_result, text_query)
                # for idx, row in card.iterrows():
                #     print (row['name'])
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

                result = sc.api_call(
                    'chat.postMessage',
                    channel=channel_id,
                    username='하스봇',
                    icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
                    text=ret_text
                )
    else:
        print ('Connection Failed')

if __name__ == '__main__':
    main()