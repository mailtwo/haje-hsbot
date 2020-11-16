#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import subprocess
import datetime
import traceback
import slack
from websocket import WebSocketTimeoutException

cur_os_type = 'linux'
if sys.platform.startswith('win'):
    cur_os_type = 'win'

# import logging
# root = logging.getLogger('slack.rtm.client')
# root.setLevel(logging.DEBUG)
# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# root.addHandler(handler)


def process_message(mode, proc, msg, user=None):
    global sc
    global wc
    global global_data

    argv = msg.strip().split()
    if argv[0] == '종료':
        if proc is None:
            if cur_os_type == 'linux':
                ret = os.system('bash kill.sh')
            else:
                ret = os.system('taskkill /IM python.exe')
        else:
            if cur_os_type == 'linux':
                out = os.popen('kill -9 %i' % proc.pid).read()
            else:
                out = os.popen('taskkill /F /T /PID %i' % proc.pid).read()
            wc.chat_postMessage(
                username='하스봇엄마',
                channel=user,
                user=user,
                text=out
            )
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
        time.sleep(5)
        ret = proc.poll()
        if ret is None:
            text = '하스봇 프로세스가 시작되었습니다.'
        else:
            text = '프로세스가 알수 없는 이유로 종료되었습니다: %d' % (ret, )
        if user is not None:
            wc.chat_postMessage(
                username='하스봇엄마',
                channel=user,
                user=user,
                text=text
            )

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
        gitout = os.popen(op).read()
        wc.chat_postMessage(
            username='하스봇엄마',
            channel=user,
            user=user,
            text=gitout
        )
    
    elif argv[0] == '에러로그':
        f_str = '읽는 중 에러'
        if os.path.exists(os.path.join('database', 'error.log')):
            with open(os.path.join('database', 'error.log'), 'r', encoding='utf-8') as f:
                f_str = f.readlines()
                if len(f_str) == 0:
                    f_str = '파일이 비어있습니다.'
                else:
                    start_idx = 0
                    for start_idx in range(len(f_str)-1, -1, -1):
                        if f_str[start_idx][:5] == '=====':
                            break
                    if start_idx < 0: start_idx = 0
                    f_str = '\n'.join(f_str[start_idx:])
        else:
            f_str = 'error.log가 없습니다.'
            open(os.path.join('database', 'error.log'), 'w').close()
        wc.chat_postMessage(
            username='하스봇엄마',
            channel=user,
            user=user,
            text=f_str
        )
    elif argv[0] == '크리티컬에러로그':
        f_str = '읽는 중 에러'
        if os.path.exists(os.path.join('database', 'critical_error.log')):
            with open(os.path.join('database', 'critical_error.log'), 'r', encoding='utf-8') as f:
                f_str = f.readlines()
                if len(f_str) == 0:
                    f_str = '파일이 비어있습니다.'
                else:
                    start_idx = 0
                    for start_idx in range(len(f_str)-1, -1, -1):
                        if f_str[start_idx][:5] == '=====':
                            break
                    if start_idx < 0: start_idx = 0
                    f_str = '\n'.join(f_str[start_idx:])
        else:
            f_str = 'critical_error.log가 없습니다.'
            open(os.path.join('database', 'critical_error.log'), 'w').close()
        wc.chat_postMessage(
            channel=user,
            username='하스봇엄마',
            user=user,
            text=f_str
        )

    return proc


sc = None
wc = None
global_data = {}
def main():
    global sc
    global wc
    global global_data
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
        global_data['filter_channel'] = filter_channel

    proc = None
    try:
        sc = slack.RTMClient(token=token_id)
        wc = slack.WebClient(token_id, timeout=30)

        global_data['proc'] = proc
        global_data['mode'] = mode

        if not sc.start():
            print('Error while sc.rtm_connect()')
            return False
    except TimeoutError as e:
        pass
    except WebSocketTimeoutException as e:
        pass
    except:
        time.sleep(60)
        pass
    time.sleep(1)

@slack.RTMClient.run_on(event='open')
def init(**payload):
    global sc
    global wc
    global global_data

    if global_data['mode'] == 'release' and global_data['proc'] is None:
        proc = process_message(global_data['mode'], global_data['proc'], '시작', user=None)
        global_data['proc'] = proc

@slack.RTMClient.run_on(event='message')
def run(**payload):
    global sc
    global wc
    global global_data

    msg_info = payload['data']
    try:
        if 'user' not in msg_info or msg_info['user'][0] != 'U':
            return
        if msg_info['channel'][:2] != 'DA' and msg_info['channel'] != global_data['filter_channel']:
            return
        text = msg_info['text']
        op = '하스봇엄마!'
        if text[:len(op)] != op:
            return
        proc = process_message(global_data['mode'], global_data['proc'], text[len(op):], user=msg_info['user'])
        global_data['proc'] = proc
    except Exception as e:
        if global_data['mode'] == 'debug':
            raise e
        else:
            ret_text = []
            ret_text.append(str(sys.exc_info()[0]))
            ret_text = '\n'.join(ret_text)
            with open('motherbot_error.log', 'a+', encoding='utf-8') as f:
                f.write('===== Current time : %s =====\n' % ('{0:%Y-%m-%d_%H:%M:%S}'.format(datetime.datetime.now()),))
                f.write('Exception occurred while exception handling!\n')
                f.write(ret_text)
                f.write(traceback.format_exc())
                f.flush()


if __name__ == '__main__':
    main()