import os
import sys
import json
import subprocess
from slackclient import SlackClient

cur_os_type = 'linux'
if sys.platform.startswith('win'):
    cur_os_type = 'win'

def process_message(mode, proc, msg):
    argv = msg.strip().split()
    if argv[0] == '종료':
        if proc is None:
            if cur_os_type == 'linux':
                ret = os.system('bash kill.sh')
            else:
                ret = os.system('taskkill /F /T /PID %i' % proc.pid)
        else:
            if cur_os_type == 'linux':
                ret = os.system('kill -9 %i' % proc.pid)
            else:
                ret = os.system('taskkill /F /T /PID %i' % proc.pid)
        if ret > 0:
            print('Kill op ret: %d' % ret)
        else:
            proc = None

    elif argv[0] == '시작':
        op = ['python', 'hs_bot_run.py', mode]
        if mode == 'debug':
            print (op)

        if cur_os_type == 'win':
            op[0] = op[0] + '.exe'
        else:
            op[0] = op[0] + '3.6'
        proc = subprocess.Popen(op, shell=False)

    elif argv[0] == '재시작':
        if proc is None:
            os.system('bash kill.sh')
        else:
            proc.kill()
        op = ['python', 'hs_bot_run.py', mode]
        if cur_os_type == 'win':
            op[0] = op[0] + '.exe'
        else:
            op[0] = op[0] + '3.6'
        if mode == 'debug':
            print (op)
        proc = subprocess.Popen(op, shell=False)

    elif argv[0] == '업데이트':
        op = 'git pull ' + ' '.join(argv[1:])
        if mode == 'debug':
            print (op)
        os.system(op)

    return proc


def main():
    mode = 'debug'
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
            if msg_info['channel'][:2] != 'DA' and msg_info['channel'] != filter_channel:
                continue
            text = msg_info['text']
            op = '하스봇엄마!'
            if text[:len(op)] != op:
                continue
            proc = process_message(mode, proc, text[len(op):])


if __name__ == '__main__':
    main()