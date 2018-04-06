import sys
from bot_manager import BotManager

card_db_col = ['inven_index', 'name', 'eng_name', 'hero', 'type', 'cost', 'attack', 'health', 'rarity', 'expansion', 'img_url', 'detail_url']

def main():
    mode = 'release'
    if len(sys.argv) == 2:
        mode = sys.argv[1].lower()
        if mode != 'release' and mode != 'debug':
            print('Invalid mode %s: should be \'debug\' or \'release\'' % (mode, ))
            return
    print('Mode: %s'% (mode, ))

    bot_mgr = BotManager(mode)
    success = bot_mgr.load_bot_token('bot_token.json')
    if not success:
        print('Terminate program...')
        return 1
    success = bot_mgr.connect()
    if not success:
        print('Terminate program...')
        return 1
    bot_mgr.run()


if __name__ == '__main__':
    main()