import os
from selenium import webdriver
import urllib.request
from bs4 import BeautifulSoup
import time
import pandas as pd

translate_table = {
    'type': {
        '주문': 'spell',
        '하수인': 'minion',
        '무기': 'weapon'
    },
    'hero': {
        '드루이드': 'druid',
        '사냥꾼': 'hunter',
        '마법사': 'mage',
        '성기사': 'paladin',
        '사제': 'priest',
        '도적': 'rogue',
        '주술사': 'shaman',
        '흑마법사': 'warlock',
        '전사': 'warrior',
        '중립': 'neutral',
        '진영': 'neutral'
    }
}

card_db_col = ['uid', 'inven_index', 'name', 'eng_name', 'hero', 'type', 'cost', 'attack', 'health', 'rarity', 'expansion', 'img_url', 'detail_url']

def initial_db():
    card_db = pd.DataFrame([[0, 0, 'None', 'None', 'None', 'None', 0, 0, 0, 'None', 'None', 'None', 'None']], columns=card_db_col)
    alias_db = pd.DataFrame([[0, 'None']], columns=['db_index', 'alias'])
    return card_db, alias_db

def main():
    db_root = '.'
    index_path = 'card_info.pd'
    alias_path = 'alias.pd'

    card_db, alias_db = initial_db()
    if os.path.exists(index_path):
        card_db = pd.read_hdf(index_path)
    if os.path.exists(alias_path):
        alias_db = pd.read_hdf(alias_path)

    card_db = start_crawling(card_db, db_root)
    for index, row in card_db.iterrows():
        card_db.at[index, 'uid'] = int(index)
    card_db.to_hdf(index_path, 'df', mode='w', format='table', data_columns=True)
    alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)


def start_crawling(db_data, db_root):
    target_expansion = []
    base_url = 'http://hs.inven.co.kr/dataninfo/card/#'
    info_url = 'http://hs.inven.co.kr/dataninfo/card/detail.php?code='

    target_str = [('expansion=' + '_'.join(map(str, target_expansion)))] if len(target_expansion) > 0 else []
    page_idx = 1

    options = webdriver.ChromeOptions()
    options.set_headless()
    driver = webdriver.Chrome(executable_path='chromedriver.exe', chrome_options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(3)

    possible_data = []
    while(True):
        card_list, card_names, card_eng_names, is_end = retrieve_card_idx(driver, base_url + ','.join(target_str + ['page=' + str(page_idx)]))
        driver.get('about:blank')
        for card_id, card_name, card_eng_name in zip(card_list, card_names, card_eng_names):
            if db_data.query('inven_index == ' + str(card_id)).empty:
                detail_url = info_url + str(card_id)
                card_info, err = retrieve_card_information(detail_url, card_id, card_name)
                if err:
                    continue
                # index_key = str([card_info['cost'], card_info['attack'], card_info['health']])
                index_data = {  'inven_index': int(card_info['index']),
                                'name': card_info['name'],
                                'eng_name': card_eng_name,
                                'hero': card_info['hero'],
                                'type': card_info['type'],
                                'cost': card_info['cost'],
                                'attack': card_info['attack'],
                                'health': card_info['health'],
                                'rarity': card_info['rarity'],
                                'expansion': card_info['expansion'],
                                'img_url': card_info['img_url'],
                                'detail_url': detail_url,
                }
                # if index_key in db_data[card_info['hero']]['index']:
                #     db_data[card_info['hero']]['index'][index_key].append(index_data)
                # else:
                #     db_data[card_info['hero']]['index'][index_key] = [index_data]
                possible_data.append(index_data)

        if is_end:
            break
        page_idx += 1

    driver.close()
    if len(possible_data) > 0:
        db_data = db_data.append([pd.DataFrame(possible_data, columns=card_db_col)], ignore_index=True)
    return db_data


def retrieve_card_idx(driver, target_url):
    print (target_url)
    while(True):
        try:
            driver.get(target_url)
            time.sleep(1)
            break
        except:
            time.sleep(5)

    inner_html = driver.execute_script('return document.body.innerHTML')
    inner_soup = BeautifulSoup(inner_html, 'html5lib')
    card_table = inner_soup.find('table', {'class': 'hsDbCardTable hsDbCardList'})
    cards = card_table.find_all('td', {'class': 'left'})
    card_ids = []
    card_names = []
    card_eng_names = []
    for card in cards:
        ahref_list = card.find_all('a')
        ahref = ahref_list[0]
        ahref_eng = ahref_list[1]
        card_ids.append(int(ahref.attrs['hs-card']))
        card_text = ahref.find('b')
        card_names.append(card_text.text)
        eng_text = ahref_eng.text
        eng_text = eng_text.replace('\'', '')
        card_eng_names.append(eng_text)
    totalnum = inner_soup.find('span', {'class': 'totalnum'})
    lastnum = inner_soup.find('span', {'class': 'lastnum'})
    while (len(totalnum.text) == 0 or len(lastnum.text) == 0):
        time.sleep(1)
        inner_html = driver.execute_script('return document.body.innerHTML')
        inner_soup = BeautifulSoup(inner_html, 'html5lib')
        totalnum = inner_soup.find('span', {'class': 'totalnum'})
        lastnum = inner_soup.find('span', {'class': 'lastnum'})
    end = (int(totalnum.text) == int(lastnum.text))
    print (card_ids)
    if end:
        print('end!')
    #page_file.close()
    return card_ids, card_names, card_eng_names, end


def retrieve_card_information(target_url, card_id, card_name):
    print (target_url)
    card_info = {
        'hero': 'druid',
        'index': card_id,
        'cost': 0,
        'attack': 0,
        'health': 0,
        'img_url': '',
        'name': card_name,
        'expansion': '오리지날',
        'type': '',
        'rarity': ''
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


    detail_table = inner_soup.find('div', {'class': 'detail-right-content'})
    detail_table = detail_table.find('tbody').find_all('tr')
    for row in detail_table:
        heads = row.find_all('th')
        values = row.find_all('td')
        for head, value in zip(heads, values):
            head = head.text
            value = value.text
            if head == '종류':
                if value not in translate_table['type']:
                    is_err = True
                    break
                card_info['type'] = translate_table['type'][value]
            elif head == '직업제한':
                card_info['hero'] = translate_table['hero'][value]
            elif head == '등급':
                card_info['rarity'] = value
            elif head == '마나비용' and value != '-':
                card_info['cost'] = int(value)
            elif head == '공격력' and value != '-':
                card_info['attack'] = int(value)
            elif head == '생명력' and value != '-':
                card_info['health'] = int(value)
            elif head == '확장팩':
                card_info['expansion'] = value
        if is_err:
            break

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