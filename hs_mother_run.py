import os
import sys
import json
import time
import subprocess
import datetime
import traceback
from slackclient import SlackClient
from websocket import WebSocketTimeoutException

cur_os_type = 'linux'
if sys.platform.startswith('win'):
    cur_os_type = 'win'

def process_message(mode, sc, proc, msg, user=None):
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
            sc.api_call(
                'chat.postMessage',
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
        sc.api_call(
            'chat.postMessage',
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
        sc.api_call(
            'chat.postMessage',
            username='하스봇엄마',
            channel=user,
            user=user,
            text=gitout
        )
    
    elif argv[0] == '에러로그':
        f_str = '읽는 중 에러'
        if os.path.exists('error.log'):
            with open('error.log', 'r', encoding='utf-8') as f:
                f_str = f.read()
        else:
            f_str = 'error.log가 없습니다.'
        sc.api_call(
            'chat.postMessage',
            username='하스봇엄마',
            channel=user,
            user=user,
            text=f_str
        )
    elif argv[0] == '크리티컬에러로그':
        f_str = '읽는 중 에러'
        if os.path.exists('critical_error.log'):
            with open('critical_error.log', 'r', encoding='utf-8') as f:
                f_str = f.read()
        else:
            f_str = 'critical_error.log가 없습니다.'
        sc.api_call(
            'chat.postMessage',
            channel=user,
            username='하스봇엄마',
            user=user,
            text=f_str
        )

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

    proc = None
    while True:
        try:
            sc = SlackClient(token_id)
            if not sc.rtm_connect():
                print('Error while sc.rtm_connect()')
                return False
            if mode == 'release' and proc is None:
                proc = process_message(mode, sc, proc, '시작', user=None)
            while sc.server.connected:
                msg_list = sc.rtm_read()
                try:
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
                        proc = process_message(mode, sc, proc, text[len(op):], user=msg_info['user'])
                    time.sleep(0.1)
                except Exception as e:
                    if mode == 'debug':
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
        except TimeoutError as e:
            pass
        except WebSocketTimeoutException as e:
            pass
        time.sleep(1)



if __name__ == '__main__':
    main()