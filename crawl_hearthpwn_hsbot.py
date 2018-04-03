import os
import re
import time
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd

card_db_col = ['inven_index', 'name', 'hero', 'type', 'cost', 'attack', 'health', 'rarity', 'expansion', 'img_url', 'detail_url']

def initial_db():
    card_db = pd.DataFrame([[0, 'None', 'None', 'None', 0, 0, 0, 'None', 'None', 'None', 'None']], columns=card_db_col)
    alias_db = pd.DataFrame([[0, 'None']], columns=['db_index', 'alias'])
    return card_db, alias_db

def main():
    db_root = '.'
    index_path = 'card_info.pd'
    alias_path = 'alias.pd'

    card_db = pd.read_hdf(index_path)
    alias_db = pd.read_hdf(alias_path)

    card_db = start_crawling(card_db, db_root)

    card_db.to_hdf(index_path, 'df', mode='w', format='table', data_columns=True)
    alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)

def start_crawling(card_db, db_root):
    target_expansion = []
    base_url = 'https://www.hearthpwn.com/cards?display=3&filter-premium=1&'

    target_str = [('expansion=' + '_'.join(map(str, target_expansion)))] if len(target_expansion) > 0 else []
    page_idx = 1

    while(True):
        card_img_list, card_names, is_end = retrieve_images(base_url + ','.join(target_str + ['page=' + str(page_idx)]))
        for img_url, name in zip(card_img_list, card_names):
            name = name.replace('-', ' ')
            name = name.capitalize()
            name = re.sub(r'([ ][a-z])', lambda x: x.group(1).upper(), name)
            query_result = card_db.query('eng_name == \"%s\"' % (name, ))
            if query_result.empty:
                continue

            card_db.at[query_result.iloc[0]['uid'], 'img_url'] = img_url
        if is_end:
            break
        page_idx += 1

    return card_db

def retrieve_images(target_url):
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
    cards = inner_soup.find_all('div', {'class': 'card-image-item'})

    card_urls = []
    card_names = []
    for card in cards:
        ahref = card.find('a')
        name = ahref.attrs['href']
        name = name[name.rfind('/')+1:]
        name = name[name.find('-') + 1:]

        imgref = ahref.find('img')
        img_url = imgref.attrs['src']

        card_urls.append(img_url)
        card_names.append(name)

    page_parent = inner_soup.find('ul', {'class': 'b-pagination-list paging-list j-tablesorter-pager j-listing-pagination'})
    last_button = page_parent.find_all('li', {'class': 'b-pagination-item'})[-1]
    end = (last_button.text != 'Next')
    print (card_names)
    if end:
        print('end!')

    return card_urls, card_names, end

if __name__ == '__main__':
    main()