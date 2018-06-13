import sys
import time
import datetime
import traceback
from bot_manager import BotManager, MsgPair
from websocket import WebSocketTimeoutException

def run_program(mode):
    critical_writen = False
    bot_mgr = BotManager(mode)
    success = bot_mgr.load_bot_token('bot_token.json')
    if not success:
        return 1
    is_init = False
    err_count = 1
    err_start = 0
    while True:
        try:
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
                    if critical_writen == False:
                        with open('critical_error.log', 'a+') as f:
                            f.write('===== Current time : %s =====\n' % ('{0:%Y-%m-%d_%H:%M:%S}'.format(datetime.datetime.now()), ))
                            f.write('System emergency stop; too many errors\n')
                            f.write('timediff: %d s' % (cur_time - err_start))
                            f.flush()
                        critical_writen = True
                    time.sleep(60)
                    err_count = 0
        except TimeoutError as e:
            is_init = False
        except WebSocketTimeoutException as e:
            is_init = False
    return 0


def main():
    mode = 'release'
    if len(sys.argv) == 2:
        mode = sys.argv[1].lower()
        if mode != 'release' and mode != 'debug':
            print('Invalid mode %s: should be \'debug\' or \'release\'' % (mode, ))
            return
    print('Mode: %s' % (mode, ))
    try:
        ret_code = run_program(mode)
        print('Return code: %d. Terminate program...' % (ret_code, ))
    except:
        if mode == 'debug':
            raise e
        else:
            ret_text = []
            ret_text.append(str(sys.exc_info()[0]))
            ret_text = '\n'.join(ret_text)
            with open('critical_error.log', 'a+', encoding='utf-8') as f:
                f.write('===== Current time : %s =====\n' % ('{0:%Y-%m-%d_%H:%M:%S}'.format(datetime.datetime.now()),))
                f.write('Exception occurred while exception handling!\n')
                f.write(ret_text)
                f.write(traceback.format_exc())
                f.flush()
            time.sleep(60)


if __name__ == '__main__':
    main()