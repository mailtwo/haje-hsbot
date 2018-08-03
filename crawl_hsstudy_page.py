target_total_page = 'https://www.hearthstudy.com/expansion/BOT'
import json
from selenium import webdriver
import os
import time
from bs4 import BeautifulSoup
import pandas as pd
from crawl_hsstudy_hsbot import retrieve_card_information, preprocess_name, translate_table
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
    inner_soup = inner_soup.find('div', {'class': 'tab-pane fade active in'})
    card_table = inner_soup.find_all('img', {'class': 'lazy-loaded img-responsive'})
    card_ids = []
    card_names = []
    card_imgs = []
    for card in card_table:
        card_ids.append(card.attrs['id'])
        card_name = card.attrs['data-original-title']
        card_names.append(card_name)

        card_imgs.append(card.attrs['data-src'])

    return card_ids, card_names, card_imgs

def crawl_total_page(page_url):
    options = webdriver.ChromeOptions()
    options.set_headless()
    driver = webdriver.Chrome(executable_path='chromedriver.exe', chrome_options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(3)

    card_list, card_names, img_list = retrieve_card_idx(driver, target_total_page)

    for card_id in card_list:
        crawl_page(card_id)

def crawl_page(card_id):
    index_data, err = crawl_card_data(card_id)
    if err:
        print (index_data)
        return
    # print('하스봇! 카드추가 ' + json.dumps(index_data, ensure_ascii=False))
    append_card(os.path.join('database', 'new_cards.pd'),
                index_data)
    print(index_data)
    print('%s 등록됨' % (card_id))

def crawl_card_data(card_id):
    target_page = 'https://www.hearthstudy.com/card/%s' % (card_id, )

    card_info, err = retrieve_card_information(target_page, '', '')
    if err:
        return card_info, err
    card_info['text'] = card_info['card_text']
    card_eng_name = target_page[target_page.rfind('/')+1:]
    card_info['img_url'] = 'https://www.hearthstudy.com/images/HD_koKR/koKR_%s.png' % (card_eng_name, )
    if 'card_text' in card_info:
        if card_info['text'][:3] == '[x]':
            card_info['text'] = card_info['text'][3:]
        card_info['text'] = card_info['text'].replace('\n', ' ').replace('$', '').replace('#', '').replace('<b>', '*').replace('</b> ', '* ').replace('</b>', '* ') \
            .replace('<i>', '_').replace('</i> ', '_ ').replace('<p>', '').replace('</p>', '')
    else:
        card_info['text'] = ''
    card_info['text'] = card_info['text'].replace(chr(160), chr(32))
    card_info['name'] = card_info['name'].replace(chr(160), chr(32))
    mechanics_list = []
    index_data = {  #'web_id': card_info['id'],
                    'orig_name': card_info['name'],
                    'name': preprocess_name(card_info['name']),
                    'eng_name': preprocess_name(card_info['eng_name']),
                    'card_text': card_info['text'],
                    'hero': card_info['hero'],
                    'type': card_info['type'],
                    'cost': card_info['cost'] if 'cost' in card_info else 0,
                    'attack': card_info['attack'] if 'attack' in card_info else 0,
                    'health': card_info['health'] if 'health' in card_info else (card_info['durability'] if 'durability' in card_info else 0),
                    'rarity': card_info['rarity'] if 'rarity' in card_info else '',
                    'expansion': card_info['expansion'],
                    'race': card_info['race'] if 'race' in card_info else '',
                    'img_url': card_info['img_url'],
                    'detail_url': target_page,
    }
    for k, v in translate_table['keywords'].items():
        if v in card_info['text']:
            mechanics_list.append(k)
    index_data['mechanics'] = mechanics_list
    return index_data, False

def append_card(update_pd_path, card_info):
    if os.path.exists(update_pd_path):
        new_pd = pd.read_hdf(update_pd_path)
    else:
        new_pd = pd.DataFrame([], columns=pd.read_hdf(os.path.join('database', 'card_info.pd')).columns)
    ids = new_pd['web_id']
    max_val = -1
    if len(ids) > 0:
        exp = ids[0][:ids[0].find('_')]
        for idx, c in enumerate(ids):
            cur_val = int(c[c.find('_')+1:])
            if cur_val > max_val:
                max_val = cur_val
        new_card_count = max_val + 1
    else:
        exp = 'BOOM'
        new_card_count=0
    for idx, items in new_pd.iterrows():
        for k in translate_table['keywords'].keys():
            if items[k] < 0.01:
                new_pd.at[idx, k] = False
            else:
                new_pd.at[idx, k] = True
    h_key = list(translate_table['keywords'].keys())
    new_pd[h_key] = new_pd[h_key].astype(bool)

    res = new_pd.query('eng_name == \"%s\"'% (card_info['eng_name']))
    if len(res) == 0:
        if 'mechanics' in card_info:
            for keywords in h_key:
                if keywords in card_info['mechanics']:
                    card_info[keywords] = True
                else:
                    card_info[keywords] = False
            del card_info['mechanics']
        card_info['web_id'] = exp + '_' + str(new_card_count)
        if new_card_count == 0:
            new_pd = pd.DataFrame([card_info], columns=pd.read_hdf(os.path.join('database', 'card_info.pd')).columns)
        else:
            new_pd = new_pd.append([pd.DataFrame([card_info])], ignore_index=True)
    else:
        print(res.iloc[0]['web_id'])
        target_idx = res.iloc[0].name
        col = new_pd.columns
        for keywords in h_key:
            new_pd.at[target_idx, keywords] = False
        for k, v in card_info.items():
            if k != 'mechanics' and k in col:
                new_pd.at[target_idx, k] = v
            elif k == 'mechanics':
                for keywords in v:
                    if keywords in col:
                        new_pd.at[target_idx, keywords] = True
    new_pd.drop_duplicates(subset='web_id', keep='last', inplace=True)
    new_pd.to_hdf(update_pd_path, 'df', mode='w', format='table', data_columns=True)

if __name__ == '__main__':
    # new_pd = pd.read_hdf(os.path.join('database', 'new_cards.pd'))
    crawl_total_page(target_total_page)
    # crawl_page('power-word-replicate')