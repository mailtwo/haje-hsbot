import os
from selenium import webdriver
import urllib.request
from bs4 import BeautifulSoup
import time
import pandas as pd

translate_table = {
    'rarity': {
        '기본': '일반',
        '일반': '일반',
        '희귀': '희귀',
        '영웅': '영웅',
        '전설': '전설'
    }
}

card_db_col = ['web_id','orig_name', 'name', 'eng_name', 'card_text', 'hero', 'type', 'cost', 'attack', 'health', 'race', 'rarity', 'expansion', 'img_url', 'detail_url']

def initial_db():
    card_db = pd.DataFrame([['None', 'None', 'None', 'None', 'None','None', 'None', 0, 0, 0, 'None', 'None', 'None','None', 'None']], columns=card_db_col)
    alias_db = pd.DataFrame([['None', 'None']], columns=['db_index', 'alias'])
    return card_db, alias_db

def main():
    db_root = '.'
    index_path = 'card_info2.pd'
    alias_path = 'alias2.pd'

    card_db, alias_db = initial_db()
    if os.path.exists(index_path):
        card_db = pd.read_hdf(index_path)
    if os.path.exists(alias_path):
        alias_db = pd.read_hdf(alias_path)

    card_db = start_crawling(card_db, db_root)

    card_db.to_hdf(index_path, 'df', mode='w', format='table', data_columns=True)
    alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)


def start_crawling(db_data, db_root):
    target_expansion = []
    base_url = 'https://www.hearthstudy.com/cards'
    info_url = 'https://www.hearthstudy.com/card/'

    target_str = [('expansion=' + '_'.join(map(str, target_expansion)))] if len(target_expansion) > 0 else []

    options = webdriver.ChromeOptions()
    options.set_headless()
    driver = webdriver.Chrome(executable_path='chromedriver.exe', chrome_options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(3)

    card_list, card_names, img_list = retrieve_card_idx(driver, base_url)
    driver.get('about:blank')

    possible_data = []
    for card_id, card_name, img_url in zip(card_list, card_names, img_list):
        ret = db_data.query('web_id == "' + str(card_id) + '"')
        if ret.empty:# or ret.iloc[0]['type'] == '무기':
            detail_url = info_url + str(card_id)
            card_info, err = retrieve_card_information(detail_url, card_id, card_name)
            if err:
                continue
            # index_key = str([card_info['cost'], card_info['attack'], card_info['health']])
            index_data = {  'web_id': card_info['id'],
                            'orig_name': card_info['name'],
                            'name': preprocess_name(card_info['name']),
                            'eng_name': card_info['eng_name'],
                            'card_text': card_info['card_text'],
                            'hero': card_info['hero'],
                            'type': card_info['type'],
                            'cost': card_info['cost'],
                            'attack': card_info['attack'],
                            'health': card_info['health'],
                            'rarity': card_info['rarity'],
                            'expansion': card_info['expansion'],
                            'img_url': img_url,
                            'detail_url': detail_url,
            }
            # if index_key in db_data[card_info['hero']]['index']:
            #     db_data[card_info['hero']]['index'][index_key].append(index_data)
            # else:
            #     db_data[card_info['hero']]['index'][index_key] = [index_data]
            possible_data.append(index_data)

    driver.close()
    if len(possible_data) > 0:
        db_data = db_data.append([pd.DataFrame(possible_data, columns=card_db_col)], ignore_index=True)
        db_data.drop_duplicates(subset='web_id', keep='last', inplace=True)
    return db_data


def preprocess_name(card_name):
    return card_name.replace(' ', '').replace('\'', '').replace(',', '')

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