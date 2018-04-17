import os
from selenium import webdriver
import urllib.request
from bs4 import BeautifulSoup
import time
import pandas as pd
import json

translate_table = {
    'hero': {
        'DRUID': '드루이드',
        'HUNTER': '사냥꾼',
        'MAGE': '마법사',
        'PALADIN': '성기사',
        'PRIEST': '사제',
        'ROGUE': '도적',
        'SHAMAN': '주술사',
        'WARLOCK': '흑마법사',
        'WARRIOR': '전사',
        'NEUTRAL': '중립',
        'DREAM': '꿈',
        'DEATHKNIGHT': '죽음의 기사'
    },

    'type': {
        'HERO': '영웅 교체',
        'MINION': '하수인',
        'SPELL': '주문',
        'WEAPON': '무기',
        'HERO_POWER': '영웅 능력'
    },

    'rarity': {
        'COMMON': '일반',
        'FREE': '일반',
        'RARE': '희귀',
        'EPIC': '영웅',
        'LEGENDARY': '전설'
    },
    'extension': {
        'TGT': '대 마상시합',
        'OG': '고대 신',
        'KARA': '카라잔',
        'GANGS': '가젯잔',
        'UNGORO': '운고로',
        'ICECROWN': '얼어붙은 왕좌',
        'LOOTAPALOOZA': '코볼트',
        'CORE': '기본',
        'EXPERT1': '오리지널',
        'HOF': '명예의 전당',
        'NAXX': '낙스라마스',
        'GVG': '고블린 대 노움',
        'BRM': '검은바위 산',
        'LOE': '탐험가 연맹',
        'GILNEAS': '마녀숲'
    },
    'race': {
        'MURLOC': '멀록',
        'DEMON': '악마',
        'BEAST': '야수',
        'DRAGON': '용족',
        'TOTEM': '토템',
        'PIRATE': '해적',
        'MECHANICAL': '기계',
        'ELEMENTAL': '정령',
        'ALL': '모두'
    }
}

card_db_col = ['web_id','orig_name', 'name', 'eng_name', 'card_text', 'hero', 'type', 'cost', 'attack', 'health', 'race', 'rarity', 'expansion', 'img_url', 'detail_url']

force_run = True
save_db = True

def initial_db():
    card_db = pd.DataFrame([['None', 'None', 'None', 'None', 'None','None', 'None', 0, 0, 0, 'None', 'None', 'None','None', 'None']], columns=card_db_col)
    alias_db = pd.DataFrame([['None', 'None']], columns=['web_id', 'alias'])
    return card_db, alias_db

def main():
    db_root = '.'
    index_path = os.path.join('database', 'card_info.pd')
    alias_path = os.path.join('database', 'alias.pd')

    card_db, alias_db = initial_db()
    if os.path.exists(index_path):
        card_db = pd.read_hdf(index_path)
    if os.path.exists(alias_path):
        alias_db = pd.read_hdf(alias_path)

    card_db = start_crawling(card_db, db_root)

    if save_db:
        card_db.to_hdf(index_path, 'df', mode='w', format='table', data_columns=True)
        if not os.path.exists(alias_path):
            alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)
    else:
        print('DB is not saved!')


def start_crawling(db_data, db_root):
    target_expansion = []
    base_url = 'https://www.hearthstudy.com/cards'
    info_url = 'https://www.hearthstudy.com/card/'

    card_data = {}
    with open(os.path.join('database', 'cards.json'), 'r', encoding='utf-8') as f:
        with open(os.path.join('database', 'cards_en.json'), 'r', encoding='utf-8') as f2:
            total_db = json.load(f)
            en_db = json.load(f2)

            for card in total_db:
                card_data[card['id']] = card
            for card in en_db:
                if 'name' in card:
                    card_data[card['id']]['eng_name'] = card['name']

    target_str = [('expansion=' + '_'.join(map(str, target_expansion)))] if len(target_expansion) > 0 else []

    use_crawling = False

    if use_crawling:
        options = webdriver.ChromeOptions()
        options.set_headless()
        driver = webdriver.Chrome(executable_path='chromedriver.exe', chrome_options=options, service_log_path=os.path.devnull)
        driver.implicitly_wait(3)

        card_list, card_names, img_list = retrieve_card_idx(driver, base_url)
        driver.get('about:blank')
    else:
        card_list = []
        card_names = []
        img_list = []
        for k, v in card_data.items():
            card_list.append(k)
            card_names.append(v['name'] if 'name' in v else '')
            img_list.append('https://www.hearthstudy.com/images/HD_koKR/koKR_%s.png' % (k, ))

    possible_data = []
    name_dict = {}

    for card_id, card_name, img_url in zip(card_list, card_names, img_list):
            card_info = card_data[card_id]
            card_info['img_url'] = img_url
            if 'type' in card_info and (card_info['type'] == 'ENCHANTMENT'):
                continue
            if 'set' in card_info and (card_info['set'] == 'HERO_SKINS' or card_info['set'] == 'TB' or card_info['set'] == 'CREDITS' or card_info['set'] == 'MISSIONS'):
                continue
            if 'name' not in card_info:
                continue
            if 'text' in card_info:
                if card_info['text'][:3] == '[x]':
                    card_info['text'] = card_info['text'][3:]
                card_info['text'] = card_info['text'].replace('\n', ' ').replace('$', '').replace('#', '').replace('<b>', '*').replace('</b> ', '* ').replace('</b>', '* ') \
                    .replace('<i>', '_').replace('</i> ', '_ ').replace('</i>', '_ ')
            else:
                card_info['text'] = ''
            if '_BOSS_' in card_info['id']:
                continue
            card_info['text'] = card_info['text'].replace(chr(160), chr(32))
            card_info['name'] = card_info['name'].replace(chr(160), chr(32))

            if card_info['id'] in ['EX1_050', 'EX1_295', 'EX1_620']:
                card_info['set'] = 'HOF'

            if card_info['name'] not in name_dict.keys():
                name_dict[card_info['name']] = [card_info]
            else:
                name_dict[card_info['name']].append(card_info)

    card_info_list = []
    for k, v in name_dict.items():
        if len(v) > 1:
            count_collectible = 0

            saved_c_idx = -1
            for c_idx, c in enumerate(v):
                if 'collectible' in c and c['collectible']:
                    count_collectible += 1
                    saved_c_idx = c_idx
            if count_collectible == 0:
                min_idx = -1
                min_str = None
                for c_idx, c in enumerate(v):
                    cur_str = c['id'].lower()
                    if cur_str[:3] == 'at_':
                        continue
                    if min_idx < 0 or min_str > cur_str:
                        min_idx = c_idx
                        min_str = cur_str
                if min_idx < 0:
                    min_idx = 0
                card_info_list.append(v[min_idx])
            elif count_collectible == 1:
                card_info_list.append(v[saved_c_idx])
            else:
                assert False
        else:
            card_info_list.append(v[0])

    for card_info in card_info_list:
        card_id = card_info['id']

        is_proceed = True
        if not force_run:
            ret = db_data.query('web_id == "' + str(card_id) + '"')
            is_proceed = bool(ret.empty)
        if is_proceed:# or ret.iloc[0]['type'] == '무기':
            detail_url = info_url + str(card_id)
            #card_info, err = retrieve_card_information(detail_url, card_id, card_name)
            card_info = card_data[card_id]

            # index_key = str([card_info['cost'], card_info['attack'], card_info['health']])
            index_data = {  'web_id': card_info['id'],
                            'orig_name': card_info['name'],
                            'name': preprocess_name(card_info['name']),
                            'eng_name': card_info['eng_name'],
                            'card_text': card_info['text'],
                            'hero': translate_table['hero'][card_info['cardClass']],
                            'type': translate_table['type'][card_info['type']],
                            'cost': card_info['cost'] if 'cost' in card_info else 0,
                            'attack': card_info['attack'] if 'attack' in card_info else 0,
                            'health': card_info['health'] if 'health' in card_info else (card_info['durability'] if 'durability' in card_info else 0),
                            'rarity': translate_table['rarity'][card_info['rarity']] if 'rarity' in card_info else '',
                            'expansion': translate_table['extension'][card_info['set']],
                            'race': translate_table['race'][card_info['race']] if 'race' in card_info else '',
                            'img_url': card_info['img_url'],
                            'detail_url': detail_url,
            }
            # if index_key in db_data[card_info['hero']]['index']:
            #     db_data[card_info['hero']]['index'][index_key].append(index_data)
            # else:
            #     db_data[card_info['hero']]['index'][index_key] = [index_data]
            possible_data.append(index_data)
    if use_crawling:
        driver.close()
    if len(possible_data) > 0:
        db_data = db_data.append([pd.DataFrame(possible_data, columns=card_db_col)], ignore_index=True)
        db_data.drop_duplicates(subset='web_id', keep='last', inplace=True)
    return db_data


def preprocess_name(card_name):
    table = str.maketrans(dict.fromkeys(' \'\",!?<>();/=+-:[]{}*&^%$#@`~\\|'))
    return card_name.translate(table)

def retrieve_card_idx(driver, target_url):
    print (target_url)
    while(True):
        try:
            driver.get(target_url)
            time.sleep(5)
            break
        except:
            time.sleep(5)

    inner_html = driver.execute_script('return document.body.innerHTML')
    #print (inner_html)
    inner_soup = BeautifulSoup(inner_html, 'html5lib')
    card_table = inner_soup.find_all('img', {'class': 'img-responsive card-img linkcard'})
    card_ids = []
    card_names = []
    card_imgs = []
    for card in card_table:
        card_ids.append(card.attrs['id'])
        card_name = card.attrs['data-original-title']
        card_names.append(card_name)

        card_imgs.append(card.attrs['data-src'])

    return card_ids, card_names, card_imgs


def retrieve_card_information(target_url, card_id, card_name):
    print (target_url)
    card_info = {
        'hero': 'druid',
        'id': card_id,
        'cost': 0,
        'attack': 0,
        'health': 0,
        'img_url': '',
        'name': card_name,
        'eng_name': '',
        'expansion': '오리지날',
        'type': '',
        'race': '',
        'rarity': '',
        'card_text': ''
    }
    is_err = False

    while(True):
        try:
            url_file = urllib.request.urlopen(target_url)
            break
        except:
            time.sleep(5)

    inner_html = url_file.read().decode('utf8')
    url_file.close()
    inner_soup = BeautifulSoup(inner_html, 'html5lib')
    card_info['img_url'] = 'None'

    for elem in inner_soup.find_all('p', {'class': 'description'}):
        if elem.text != 'Related Contents':
            card_info['eng_name'] = elem.text

    detail_table = inner_soup.find('div', {'class': 'panel panel-default'})
    card_info['hero'] = detail_table.find('div', {'class': 'panel-heading'}).find('a').text
    info_list = detail_table.find_all('div', {'class': 'panel-body'})
    for info_data in info_list:
        if info_data.find('a') is not None:
            ahref = info_data.find('a')
            href_url = ahref.attrs['href']
            value = ahref.text
            if 'rarity' in href_url:
                card_info['rarity'] = translate_table['rarity'][value]
            elif 'type' in href_url:
                card_info['type'] = value
            elif 'race' in href_url:
                card_info['race'] = value
            elif 'category' in href_url:
                card_info['expansion'] = value
        else:
            img_list = info_data.find_all('img')
            stat_list = info_data.find_all('h3')
            stat_idx = 0
            for img in img_list:
                stat = stat_list[stat_idx]
                while len(stat.text) == 0:
                    stat_idx += 1
                    stat = stat_list[stat_idx]
                img_src = img.attrs['src']
                if 'mana' in img_src:
                    card_info['cost'] = int(stat.text)
                elif 'attack' in img_src or 'WEAPON' in img_src:
                    card_info['attack'] = int(stat.text)
                elif 'MINION' in img_src or 'health_weapon' in img_src:
                    card_info['health'] = int(stat.text)
                stat_idx += 1

    bs_callout = detail_table.parent.find('div', {'class':'bs-callout'})
    if bs_callout is not None:
        card_text_elem = bs_callout.find('p')
        card_info['card_text'] = card_text_elem.text

    return card_info, is_err


def download_img(target_url, save_path):
    is_err = False
    try:
        if not os.path.exists(save_path):
            urllib.request.urlretrieve(target_url, save_path)
    except:
        print('Fail to download url %s to %s'%(target_url, save_path))
        is_err = True
    return is_err


if __name__ == '__main__':
    main()