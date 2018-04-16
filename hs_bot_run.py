import sys
import time
import datetime
from bot_manager import BotManager, MsgPair

def run_program(mode):
    bot_mgr = BotManager(mode)
    success = bot_mgr.load_bot_token('bot_token.json')
    if not success:
        return 1
    is_init = False
    err_count = 1
    err_start = 0
    while True:
        success = bot_mgr.connect()
        if not success:
            return 1

        if is_init:
            msg_pair = MsgPair('simple_txt', '하스봇이 재시작 되었습니다.')
            bot_mgr.send_msg_pair(msg_pair)
        is_init = True

        ret = bot_mgr.run()
        if ret == 0:
            break
        else:
            bot_mgr.close()
            cur_time = time.time()
            if err_start == 0 or cur_time - err_start > 60.:
                err_start = cur_time
                err_count = 1
            else:
                err_count += 1
            if err_count >= 3:
                with open('critical_error.log', 'a+') as f:
                    f.write('===== Current time : %s =====\n' % ('{0:%Y-%m-%d_%H:%M:%S}'.format(datetime.datetime.now()), ))
                    f.write('System emergency stop; too many errors\n')
                    f.write('timediff: %d s' % (cur_time - err_start))
                    f.flush()
                return 1
    return 0


def main():
    mode = 'release'
    if len(sys.argv) == 2:
        mode = sys.argv[1].lower()
        if mode != 'release' and mode != 'debug':
            print('Invalid mode %s: should be \'debug\' or \'release\'' % (mode, ))
            return
    print('Mode: %s' % (mode, ))
    ret_code = run_program(mode)
    print('Return code: %d. Terminate program...' % (ret_code, ))


if __name__ == '__main__':
    main()