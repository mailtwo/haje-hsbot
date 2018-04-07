import sys
from bot_manager import BotManager

def run_program(mode):
    bot_mgr = BotManager(mode)
    success = bot_mgr.load_bot_token('bot_token.json')
    if not success:
        return 1
    success = bot_mgr.connect()
    if not success:
        return 1
    bot_mgr.run()
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