import os
import sys
import json
import subprocess
from slackclient import SlackClient


def process_message(proc, msg):
    argv = msg.strip().split()
    if argv[0] == '종료':
        if proc is None:
            os.system('bash kill.sh')
        else:
            proc.kill()

    elif argv[0] == '시작':
        proc = subprocess.Popen(['python3.6', 'hs_bot_run.py', 'release'], shell=True)
    elif argv[0] == '재시작':
        if proc is None:
            os.system('bash kill.sh')
        else:
            proc.kill()
        proc = subprocess.Popen(['python', 'hs_bot_run.py', 'release'], shell=True)
    elif argv[0] == '업데이트':
        os.system('git pull ' + ' '.join(argv[1:]))

    return proc


def main():
    mode = 'release'
    if len(sys.argv) == 2:
        mode = sys.argv[1].lower()
        if mode != 'release' and mode != 'debug':
            print('Invalid mode %s: should be \'debug\' or \'release\'' % (mode, ))
            return
    print('Mode: %s' % (mode, ))
    if mode == 'debug':
        target_channel_name = 'bottest'
    else:
        target_channel_name = 'game_hs'

    path = 'bot_token.json'
    with open(path, 'r') as f:
        token_data = json.load(f)
        token_id = token_data['token_id']
        channel_id = token_data['channels'][target_channel_name]
        filter_channel = channel_id

    sc = SlackClient(token_id)
    if not sc.rtm_connect():
        print('Error while sc.rtm_connect()')
        return False
    sc.server.websocket.sock.setblocking(1)
    proc = None
    while sc.server.connected:
        msg_list = sc.rtm_read()
        for msg_info in msg_list:
            if msg_info['type'] != 'message':
                continue
            if 'user' not in msg_info or msg_info['user'][0] != 'U':
                continue
            if msg_info['channel'][:2] != 'DA':
                continue
            text = msg_info['text']
            op = '하스봇엄마!'
            if text[:len(op)] != op:
                continue
            proc = process_message(proc, text[len(op):])


if __name__ == '__main__':
    main()