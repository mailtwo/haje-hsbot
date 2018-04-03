import json
from slackclient import SlackClient
from db_connector import DBConnector

card_db_col = ['inven_index', 'name', 'eng_name', 'hero', 'type', 'cost', 'attack', 'health', 'rarity', 'expansion', 'img_url', 'detail_url']

def main():
    target_channel_name = 'bottest'
    with open('bot_token.json', 'r') as f:
        token_data = json.load(f)
        token_id = token_data['token_id']
        channel_id = token_data['channels'][target_channel_name]
    print('Bot token: %s' % (str(token_id), ))
    print('Channel: %s (%s)' % (target_channel_name, str(channel_id)))
    slack_token = token_id

    db = DBConnector()
    db.load('card_info.pd', 'alias.pd')

    sc = SlackClient(slack_token)

    if sc.rtm_connect():
        sc.server.websocket.sock.setblocking(1)
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
                if text[:2] == '[[' and text[-2:] == ']]':
                    card = db.query_info({'name': text[2:-2]})
                    ret_text = ''
                    if card.empty:
                        ret_text = '%s 에 해당하는 카드를 찾을 수 없습니다.' % (text, )
                    elif card.shape[0] == 1:
                        cur_data = card.iloc[0]
                        ret_text = '<%s|%s>\n%s'% (cur_data['detail_url'], '[[' + cur_data['name'] + ']]', cur_data['img_url'])
                    else:
                        ret_text = []
                        for idx in range(card.shape[0]):
                            ret_text.append('<%s|%s>' % (card.iloc[idx, card_db_col.index('detail_url')],
                                                        '[[' + card.iloc[idx, card_db_col.index('name')] + ']]'))
                        ret_text = ', '.join(ret_text)
                    result = sc.api_call(
                        "chat.postMessage",
                        channel="#bottest",
                        username='하스봇',
                        icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
                        text=ret_text
                    )
                    print(result)
    else:
        print ("Connection Failed")

    # result = sc.api_call(
    #     "chat.postMessage",
    #     channel="#bottest",
    #     username='하스봇',
    #     icon_url='https://emoji.slack-edge.com/T025GK74E/hearthstone/589f51fac849905f.png',
    #     text="봇 접속 테스트"
    # )
    # print(result)

if __name__ == '__main__':
    main()