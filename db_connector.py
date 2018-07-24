import os
import re
import pandas as pd

alias_db_col = ['web_id', 'alias']
# hs_keywords = ['은신', '도발', '돌진', '질풍', '광풍', '빙결', '침묵', '주문 공격력', '차단', '천상의 보호막', '독성',
#                '전투의 함성', '전투의 함성:', '죽음의 메아리', '죽음의 메아리:', '면역', '선택 -', '선택', '연계', '연계:'
#                '과부하', '비밀', '비밀:', '예비 부품', '격려', '격려:', '창시합', '발견', '비취 골렘', '적응', '퀘스트',
#                '퀘스트:', '보상', '보상:', '생명력 흡수', '소집', '개전', '속공', '잔상']
hs_races = ['멀록', '악마', '야수', '용족', '토템', '해적', '기계', '정령', '모두']
hs_expansion_group = {
    '정확': [],
    '정규': ['코볼트', '얼어붙은 왕좌', '운고로', '마녀숲', '오리지널', '기본'],
    '야생': ['대 마상시합', '명예의 전당', '낙스라마스', '고블린 대 노움', '검은바위 산', '탐험가 연맹', '가젯잔', '카라잔', '고대 신'],
    '모험모드': ['낙스라마스 모험모드', '검은바위 산 모험모드', '탐험가 연맹 모험모드', '카라잔 모험모드', '얼어붙은 왕좌 모험모드', '코볼트 모험모드', '마녀숲 모험모드', '시간의 선술집']
}
hs_expansion_priority = ['정확', '정규', '야생', '모험모드']
hs_keywords = {
    '소집': 'RECRUIT',
    '돌진': 'CHARGE',
    '크툰': 'RITUAL',
    '비밀': 'SECRET',
    '은신': 'STEALTH',
    '엉뚱': 'FORGETFUL',
    '오라': 'AURA',
    '도발': 'TAUNT',
    '격려': 'INSPIRE',
    '질풍': 'WINDFURY',
    '잠듦': 'UNTOUCHABLE',
    '천상의 보호막': 'DIVINE_SHIELD',
    '비취': 'JADE_GOLEM',
    '격노': 'ENRAGED',
    '카운터': 'COUNTER',
    '얼림': 'FREEZE',
    '연계': 'COMBO',
    '면역': 'IMMUNE',
    '주문 공격력': 'SPELLPOWER',
    '침묵': 'SILENCE',
    '과부하': 'OVERLOAD',
    '발견': 'DISCOVER',
    '독성': 'POISONOUS',
    '죽음의 메아리': 'DEATHRATTLE',
    '선택': 'CHOOSE_ONE',
    '적응': 'ADAPT',
    '퀘스트': 'QUEST',
    '전투의 함성': 'BATTLECRY',
    '방호': 'CANT_BE_TARGETED_BY_SPELLS',
    '공격 불가': 'CANT_ATTACK',
    '생명력 흡수': 'LIFESTEAL',
    '뽑을시': 'TOPDECK',
    '잔상': 'ECHO',
    '속공': 'RUSH',
    '개전': 'START_OF_GAME',
    '예비 부품': 'SPARE_PART',
    '광풍': 'MEGA_WINDFURY',
}

class DBConnector(object):
    def __init__(self, mode):
        self.mode = mode
        self.card_db = None
        self.alias_db = None
        self.standard_filter = ['코볼트', '얼어붙은 왕좌', '운고로', '마녀숲', '오리지널', '기본']
        self.wild_filter = ['대 마상시합', '명예의 전당', '낙스라마스', '고블린 대 노움', '검은바위 산', '탐험가 연맹', '가젯잔', '카라잔', '고대 신']
        self.hero_alias = {
                            '드루이드': '드루이드',
                            '드루': '드루이드',
                            '노루': '드루이드',
                            '사냥꾼': '사냥꾼',
                            '냥꾼': '사냥꾼',
                            '마법사': '마법사',
                            '법사': '마법사',
                            '성기사': '성기사',
                            '사제': '사제',
                            '안두인': '사제',
                            '도적': '도적',
                            '주술사': '주술사',
                            '술사': '주술사',
                            '흑마법사': '흑마법사',
                            '흑마': '흑마법사',
                            '전사': '전사',
                            '중립': '중립'}
        self.expansion_alias = {'코볼트': '코볼트',
                                '얼어붙은 왕좌': '얼어붙은 왕좌',
                                '얼왕': '얼어붙은 왕좌',
                                '얼왕기': '얼어붙은 왕좌',
                                '운고로': '운고로',
                                '가젯잔': '가젯잔',
                                '카라잔': '카라잔',
                                '오리지널': '오리지널',
                                '대 마상시합': '대 마상시합',
                                '대마상': '대 마상시합',
                                '낙스라마스': '낙스라마스',
                                '낙스': '낙스라마스',
                                '고블린 대 노움': '고블린 대 노움',
                                '고대놈': '고블린 대 노움',
                                '고놈': '고블린 대 노움',
                                '검은바위 산': '검은바위 산',
                                '검바산': '검은바위 산',
                                '탐험가 연맹': '탐험가 연맹',
                                '탐연': '탐험가 연맹',
                                '마녀숲': '마녀숲',
                                '시간의 선술집': '시간의 선술집'}
        self.type_alias = {'주문': '주문',
                           '하수인': '하수인',
                           '무기': '무기',
                           '교체': '영웅 교체',
                           '영웅 교체': '영웅 교체',
                           '죽음의 기사': '영웅 교체',
                           '죽기': '영웅 교체',}
        self.rarity_alias = {'일반': '일반',
                             '희귀': '희귀',
                             '전설': '전설',
                             '영웅': '영웅',}
        self.keyword_alias = {'천보': '천상의 보호막',
                              '생흡': '생명력 흡수',
                              '주공': '주문 공격력',
                              '전함': '전투의 함성',
                              '전함:': '전투의 함성:',
                              '죽메': '죽음의 메아리',
                              '죽메:': '죽음의 메아리:',
                              '선택-': '선택 -',
                              '선택:': '선택 -'}
        self.card_db_col = ['web_id', 'name', 'eng_name', 'card_text', 'hero', 'type', 'cost', 'attack', 'health', 'race', 'rarity', 'expansion', 'img_url', 'detail_url']
        self.card_db_col += list(hs_keywords.values())
        self.new_expansion_name = '폭심만만'
        self.new_expansion_id = 'BOOM'
        self.new_expansion_img = 'https://d2q63o9r0h0ohi.cloudfront.net/images/the-boomsday-project/ko-kr/logo@2x-833c15ebac3668ec08ab3cc98d26c59dc635705af87309f4181b5f1b7922546082ca4b699b25e311301a42ec3dffb4c65b939654832a36fa0fd9ff75c5209523.png'
        self.new_card_count = 0
        self.new_card_db = None

        if self.new_expansion_name is not None:
            self.standard_filter.append(self.new_expansion_name)
            self.expansion_alias[self.new_expansion_name] = self.new_expansion_name
            hs_expansion_group['정규'].append(self.new_expansion_name)

        # self.exp_group_query = {}
        # for k, v in hs_expansion_group.items():
        #     expansions = []
        #     for exp_name in hs_expansion_group['정규']:
        #         expansions.append('expansion == \"%s\"' % (exp_name, ))
        #     expansion_query_str = ' | '.join(expansions)
        #     self.exp_group_query[k] = '( ' + expansion_query_str + ' )'

        self.parse_word_list = [
            (list(self.hero_alias.keys()), 'hero',
                lambda word: {'value': self.hero_alias[word]}
             ),
            ('([0-9]+)([-+])?코(스트)?', 'cost',
                lambda re_obj: {
                    'value': int(re_obj.group(1)),
                    'op': 'eq' if re_obj.group(2) is None else re_obj.group(2)
                }),
            ('([0-9]+)~([0-9]+)코(스트)?', 'costrange',
                lambda re_obj: {
                    'value': (int(re_obj.group(1)), int(re_obj.group(2)))
                }),
            ('([0-9]+)([-+])?[ /]([0-9]+)([-+])?', 'attackhealth',
                lambda re_obj: {
                    'value': (int(re_obj.group(1)), int(re_obj.group(3))),
                    'op': ('eq' if re_obj.group(2) is None else re_obj.group(2),
                           'eq' if re_obj.group(4) is None else re_obj.group(4))
                }),
            ('([0-9]+)([-+])?공(격력)?', 'attack',
                lambda re_obj: {
                    'value': int(re_obj.group(1)),
                    'op': 'eq' if re_obj.group(2) is None else re_obj.group(2)
                }),
            ('([0-9]+)~([0-9]+)공(격력)?', 'attackrange',
                lambda re_obj: {
                    'value': (int(re_obj.group(1)), int(re_obj.group(2)))
                }),
            ('([0-9]+)([-+])?체(력)?', 'health',
                lambda re_obj: {
                    'value': int(re_obj.group(1)),
                    'op': 'eq' if re_obj.group(2) is None else re_obj.group(2)
                }),
            ('([0-9]+)~([0-9]+)체(력)?', 'healthrange',
                lambda re_obj: {
                    'value': (int(re_obj.group(1)), int(re_obj.group(2)))
                }),
            (hs_races, 'race',
                lambda word: {'value': word}),
            (list(hs_expansion_group.keys()), 'expansion_group',
                lambda word: {'value': word}),
            (list(self.type_alias.keys()), 'type',
                lambda word: {
                    'value': self.type_alias[word]
                }),
            (list(self.rarity_alias.keys()), 'rarity',
                lambda word: {
                    'value': self.rarity_alias[word]
                }),
            ([';'], 'end_stat',
                lambda word: {'value': word}),
            (list(hs_keywords.keys()), 'keyword',
                lambda word: {'value': word}),
            (list(self.keyword_alias.keys()), 'keyword',
                lambda word: {
                    'value': self.keyword_alias[word]
                }),
            (list(self.expansion_alias.keys()), 'expansion',
                lambda word: {
                    'value': self.expansion_alias[word]
                }),
        ]
        for i in range(len(self.parse_word_list)):
            cond, word_type, ret_fun = self.parse_word_list[i]
            if type(cond) == list:
                cond_fun = self._compare_word_list_gen(cond)
                cond_type = 'list'
            elif type(cond) == str:
                cond_fun = self._compare_re_gen(cond)
                cond_type = 're'
            else:
                assert False
            self.parse_word_list[i] = (cond_fun, cond_type, word_type, ret_fun)


        default_card_data = {
            'web_id': self.new_expansion_id + '_' + str(self.new_card_count),
            'orig_name': '없음',
            'name': '없음',
            'eng_name': 'none',
            'card_text': 'empty',
            'hero': '중립',
            'type': '주문',
            'cost': 0,
            'attack': 0,
            'health': 0,
            'race': '',
            'rarity': '일반',
            'expansion': self.new_expansion_name,
            'mechanics': [],
            'img_url': self.new_expansion_img,
            'detail_url': 'https://playhearthstone.com/ko-kr/'
        }
        from crawl_hsstudy_hsbot import keyword_keys
        for k in keyword_keys:
            default_card_data['mechanics'].append(k)
        self.default_card_data = default_card_data

    def _compare_word_list_gen(self, cond_list):
        def _inner_fun(word):
            for cond in cond_list:
                if len(word) < len(cond):
                    continue
                if word[:len(cond)] != cond:
                    continue
                return True, cond, len(cond)
            return False, word, 0
        return _inner_fun

    def _compare_re_gen(self, re_str, force_at_start=True):
        regex = re.compile(('^' if force_at_start else '')+re_str + '(.*)$')
        def _inner_fun(word):
            match_obj = regex.search(word)
            if match_obj is not None:
                word_len = len(word) - len(match_obj.group(len(match_obj.groups())))
            else:
                word_len = 0
            return match_obj is not None, match_obj,word_len
        return _inner_fun


    # load DataFrame type database from the path
    def load(self, card_db_path_list, alias_db_path, new_card_db=None):
        self.card_db = []
        for card_db_path in card_db_path_list:
            assert(os.path.exists(card_db_path))
            self.card_db.append(pd.read_hdf(card_db_path))
        self.card_db = pd.concat(self.card_db)
        assert (os.path.exists(alias_db_path))
        self.alias_db = pd.read_hdf(alias_db_path)
        if new_card_db is not None:
            assert (os.path.exists(new_card_db))
            self.new_card_db = pd.read_hdf(new_card_db)
            if self.new_card_db is not None:
                ids = self.new_card_db['web_id']
                max_val = -1
                for idx, c in enumerate(ids):
                    cur_val = int(c[c.find('_')+1:])
                    if cur_val > max_val:
                        max_val = cur_val
                self.new_card_count = max_val + 1
                self.card_db = pd.concat([self.card_db, self.new_card_db])
            else:
                os.remove(new_card_db)

        #self.mem_db = self._construct_mem_db(self.card_db)
        self.mem_db = self._construct_mem_db(self.card_db)
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        # self.keyword_db = {}
        # for keyword in hs_keywords:
        #     cur_list = []
        #     for idx, row in self.card_db.iterrows():
        #         if keyword in row['card_text']:
        #             cur_list.append(row['web_id'])
        #     self.keyword_db[keyword] = cur_list

    def _construct_mem_db(self, df):
        keys = []
        names = []
        eng_names = []
        card_texts = []
        card_texts_norm = []
        for idx, row in df.iterrows():
            if row['web_id'] == 'None':
                continue
            keys.append(row['web_id'])
            names.append(row['name'])
            eng_names.append(row['eng_name'])
            card_texts.append(row['card_text'])
            card_texts_norm.append(self.normalize_text(row['card_text']))
        return {
            'key': keys,
            'name': names,
            'eng_name': eng_names,
            'text': card_texts,
            'norm_text': card_texts_norm
        }

    def _construct_alias_mem_db(self, df):
        web_ids = []
        names = []
        for idx, row in df.iterrows():
            if row['web_id'] == 'None':
                continue
            web_ids.append(row['web_id'])
            names.append(row['alias'])
        return {
            'web_id': web_ids,
            'name': names,
        }

    def _faster_isin(self, df, web_id_list):
        return df.merge(pd.DataFrame(web_id_list, columns=['web_id']), on='web_id')

    def query_stat(self, stat_query):
        assert(self.card_db is not None)
        query_str = []
        if 'expansion' not in stat_query:
            expansion = []
            if 'expansion_group' in stat_query:
                for token in stat_query['expansion_group']:
                    if token['value'] in hs_expansion_group.keys():
                        for v in hs_expansion_group[token['value']]:
                            expansion.append({'value': v, 'neg': False, 'op':'eq'})
            if len(expansion) > 0:
                stat_query['expansion'] = expansion
        if ('race' in stat_query):
            pos_inc = False
            for q in stat_query['race']:
                if q['neg'] == False:
                    pos_inc = True
                    break
            if pos_inc:
                stat_query['race'].append({'value': '모두', 'neg': False, 'op': 'eq'})
            else:
                stat_query['race'].append({'value': '모두', 'neg': True, 'op': 'eq'})

        for k, t_list in stat_query.items():
            pos_inc = False
            for token in t_list:
                if token['neg'] == False:
                    pos_inc = True
                    break

            cur_value_query = []
            if k in self.card_db_col:
                if len(t_list) == 0:
                    continue
                for token in t_list:
                    v = token['value']
                    op = '=='
                    if 'op' in token:
                        if token['op'] == '+':
                            op = '>='
                        elif token['op'] == '-':
                            op = '<='
                        elif token['op'] == 'eq':
                            op = '=='
                        else:
                            continue

                    if type(v) == int:
                        if -10000 < v < 10000:
                            cur_value_query.append('%s(%s %s %s)' % ('not ' if token['neg'] else '', k, op, str(v)))
                    else:
                        cur_value_query.append('%s(%s %s \"%s\")' % ('not ' if token['neg'] else '', k, op, v))

            elif k == 'keyword':
                for token in t_list:
                    col = hs_keywords[token['value']]
                    cur_value_query.append('%s(%s == True)' % ('not ' if token['neg'] else '', col))
                # Exceptionally, keyword queries are concatenated with ANDs
                cur_value_query = [' & '.join(cur_value_query)]

            elif k == 'attackhealth':
                for token in t_list:
                    attack, health = token['value']
                    cur_value_query.append('( %s(%s == %s & %s == %s) )' % ('not ' if token['neg'] else '',
                                                                            'attack', attack, 'health', health))

            elif k == 'costrange':
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'cost', low, 'cost', high))

            elif k == 'attackrange':
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'attack', low, 'attack', high))

            elif k == 'healthrange':
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'health', low, 'health', high))

            if len(cur_value_query) > 0:
                if pos_inc:
                    query_str.append('(' + (' | '.join(cur_value_query)) + ')')
                else:
                    query_str.append('(' + (' & '.join(cur_value_query)) + ')')


        # print (stat_query)
        if len(query_str) > 0:
            query_str = ' & '.join(query_str)
            if self.mode == 'debug':
                print (query_str)
            ret = self.card_db.query(query_str)
        else:
            ret = self.card_db

        # if 'keyword' in stat_query:
        #     for token in stat_query['keyword']:
        #         card_list = self.keyword_db[token['value']]
        #         ret = self._faster_isin(ret, card_list)

        return ret

    def query_text(self, query_table, stat_query, text_query, raw_query):
        text_query = text_query.strip()
        group_df = {}
        exactly_match = False
        exactly_key = []
        for k in hs_expansion_group.keys():
            group_df[k] = pd.DataFrame(columns=self.card_db_col)
        if len(text_query) == 0:
            if query_table is None:
                return pd.DataFrame(columns=self.card_db_col), group_df
            query_table = query_table.drop_duplicates(subset='web_id', keep='last')
            total_name_list = self.mem_db['name']
            for idx, each_name in enumerate(total_name_list):
                if raw_query == each_name:
                    exactly_key = [self.mem_db['key'][idx]]
                    exactly_match = True
        else:
            if query_table is None:
                assert(self.card_db is not None)
                cur_memdb = self.mem_db
                cur_alias_mem_db = self.alias_mem_db
            else:
                cur_memdb = self._construct_mem_db(query_table)
                #joined = pd.merge(self.alias_db, query_table[['web_id']], on='web_id', how='left')
                joined = self.alias_db.join(query_table[['web_id']].set_index('web_id'), how='inner', on='web_id')
                cur_alias_mem_db = self._construct_alias_mem_db(joined)

            check_eng = True
            for w in text_query:
                if ord(w) > 255:
                    check_eng = False
                    break
            if check_eng:
                name_list = cur_memdb['eng_name']
            else:
                name_list = cur_memdb['name']
            ret_key = []
            total_name_list = self.mem_db['name']
            for idx, each_name in enumerate(total_name_list):
                if raw_query == each_name:
                    exactly_key = [self.mem_db['key'][idx]]
                    exactly_match = True
            for idx, each_name in enumerate(name_list):
                if text_query != each_name and text_query in each_name:
                    ret_key.append(cur_memdb['key'][idx])

            name_list = cur_alias_mem_db['name']
            for idx, each_name in enumerate(name_list):
                if text_query in each_name:
                    ret_key.append(cur_alias_mem_db['web_id'][idx])

            query_table = self._faster_isin(self.card_db, ret_key)
            query_table.drop_duplicates(subset='web_id', keep='last', inplace=True)

        for k in hs_expansion_group.keys():
            group_df[k] = pd.DataFrame(columns=query_table.columns)

        if not query_table.empty:
            if ('expansion_group' not in stat_query) and ('expansion' not in stat_query):
                df_group = query_table.groupby('expansion')
                for k, group_filter in hs_expansion_group.items():
                    group_list = [df_group.get_group(x) for x in group_filter if x in df_group.groups]
                    if len(group_list) > 0:
                        group_df[k] = pd.concat(group_list)

                query_table = group_df[hs_expansion_priority[-1]]
                for k in hs_expansion_priority:
                    if not group_df[k].empty:
                        query_table = group_df[k]
                        break


        if exactly_match and \
                (('expansion_group' not in stat_query) and ('expansion' not in stat_query)):
            query_table = self._faster_isin(self.card_db, exactly_key)
            query_table.drop_duplicates(subset='web_id', keep='last', inplace=True)
            assert(hs_expansion_priority[0] == '정확')
            group_df[hs_expansion_priority[0]] = query_table
        return query_table, group_df

    def query_text_in_card_text(self, query_table, stat_query, text_query):
        text_query = text_query.strip()
        group_df = {}
        for k in hs_expansion_group.keys():
            group_df[k] = pd.DataFrame(columns=self.card_db_col)
        if len(text_query) == 0:
            if query_table is None:
                return pd.DataFrame(columns=self.card_db_col), group_df
            query_table = query_table.drop_duplicates(subset='web_id', keep='last')
        else:
            if query_table is None:
                assert(self.card_db is not None)
                cur_memdb = self.mem_db
            else:
                cur_memdb = self._construct_mem_db(query_table)

            if len(text_query) == 0:
                return query_table

            text_list = cur_memdb['norm_text']
            ret_key = []
            for idx, each_text in enumerate(text_list):
                if text_query in each_text:
                    ret_key.append(cur_memdb['key'][idx])

            query_table = self._faster_isin(self.card_db, ret_key)
            query_table.drop_duplicates(subset='web_id', keep='last', inplace=True)

        for k in hs_expansion_group.keys():
            group_df[k] = pd.DataFrame(columns=query_table.columns)

        if not query_table.empty:
            if ('expansion_group' not in stat_query) and ('expansion' not in stat_query):
                df_group = query_table.groupby('expansion')
                for k, group_filter in hs_expansion_group.items():
                    group_list = [df_group.get_group(x) for x in group_filter if x in df_group.groups]
                    if len(group_list) > 0:
                        group_df[k] = pd.concat(group_list)

                query_table = group_df[hs_expansion_priority[-1]]
                for k in hs_expansion_priority:
                    if not group_df[k].empty:
                        query_table = group_df[k]
                        break

        return query_table, group_df

    # parse the text user types and return its stat query and text query
    def parse_user_request(self, text):
        orig_text = text
        text = re.sub('\s+', ' ', text).strip()
        stat_query = {}
        text_query = ''

        parse_end = False
        is_invalid = False
        while not parse_end:
            if len(text) == 0:
                break
            res, token_info, text, err_msg = self._parse_word_test(text)
            text = text.strip()
            if err_msg is not None:
                return None, None, err_msg

            word_type = token_info['type']
            # value = token_info['value']

            if not res:
                is_invalid = True
                break
            if word_type == 'end_stat':
                text_query = self.normalize_text(text, cannot_believe=True)
                break

            # process the special case if the stat is concatenated
            # elif word_type == 'attackhealth':
            #     if 'attack' not in stat_query:
            #         stat_query['attack'] = [value[0]]
            #     else:
            #         stat_query['attack'].append(value[0])
            #     if 'health' not in stat_query:
            #         stat_query['health'] = [value[1]]
            #     else:
            #         stat_query['health'].append(value[1])
            # the normal case; type should be the database column string
            else:
                if word_type not in stat_query:
                    stat_query[word_type] = [token_info]
                else:
                    stat_query[word_type].append(token_info)

        # Here, if user query include any non-empty text query,
        # the program thinks the whole user query is for the text query
        # This prevents the situation when the first part of the normal text query
        # is the same with the shape of the stat query
        # ex) 퀘스트 중인 모험가 -> {keyword: 퀘스트} "중인 모험가" (X) "퀘스트 중인 모험가" (O)
        if is_invalid:
            stat_query = {}
            text_query = self.normalize_text(orig_text, cannot_believe=True)

        raw_query = self.normalize_text(orig_text, cannot_believe=True)
        return stat_query, text_query, raw_query, None

    def normalize_text(self, text, cannot_believe=False):
        if cannot_believe:
            table = str.maketrans(dict.fromkeys(' \'\",!?<>();/=+-:[]{}*&^%$#@`~\\|'))
            return text.translate(table).lower()
        else:
            table = str.maketrans(dict.fromkeys(' \',:*_'))
            return text.translate(table).lower()

    def insert_alias(self, card_row, card_alias):
        web_id = card_row['web_id']
        inserting_data = {
            'web_id': web_id,
            'alias': card_alias
        }
        self.alias_db = self.alias_db.append([pd.DataFrame([inserting_data], columns=alias_db_col)], ignore_index=True)
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        if self.mode == 'debug':
            print('%s (%s) -> %s 등록 완료' %(card_row['orig_name'], web_id, card_alias))

    def get_alias_list(self, card_row):
        if card_row is None:
            target_db = self.alias_db
        else:
            target_db = self.alias_db[self.alias_db.web_id == card_row['web_id']]

        ret_df = pd.merge(target_db, self.card_db[['web_id', 'orig_name']], how='left', on='web_id').set_index(target_db.index)
        ret = []
        for idx, row in ret_df.iterrows():
            if row.name == 0:
                continue
            ret.append({
                'id': row.name,
                'name': row['orig_name'],
                'alias': row['alias']
            })
        return ret

    def delete_alias(self, alias_id):
        if alias_id not in self.alias_db.index:
            return 'empty'
        self.alias_db.drop([alias_id], inplace=True)
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        return 'success'

    def update_alias(self, alias_id, update_to):
        if alias_id not in self.alias_db.index:
            return 'empty'
        self.alias_db.at[alias_id, 'alias'] = update_to
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        return 'success'

    def flush_alias_db(self):
        alias_path = os.path.join('database', 'alias.pd')
        self.alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)

    def add_card_to_db(self, card_info, id_prefix=None, update_pd_path=None):
        if id_prefix is None:
            id_prefix = self.new_expansion_id

        new_card_info = self.default_card_data.copy()
        new_card_info.update(card_info)
        if 'web_id' not in card_info:
            new_card_info['web_id'] = id_prefix+ '_' + str(self.new_card_count)
        new_card_info['name'] = self.normalize_text(new_card_info['orig_name'], cannot_believe=True)
        new_card_info['eng_name'] = self.normalize_text(new_card_info['eng_name'], cannot_believe=True)
        new_card_info['card_text'] = new_card_info['card_text']
        new_card_info['cost'] = int(new_card_info['cost'])
        new_card_info['attack'] = int(new_card_info['attack'])
        new_card_info['health'] = int(new_card_info['health'])
        if 'mechanics' in new_card_info:
            for v in new_card_info['mechanics']:
                new_card_info[v] = True
        card_info = new_card_info
        self.card_db = self.card_db.append([pd.DataFrame([card_info], columns=self.card_db.columns)], ignore_index=True)
        self.card_db.drop_duplicates(subset='web_id', keep='last', inplace=True)
        self.mem_db = self._construct_mem_db(self.card_db)
        self.new_card_count += 1

        if update_pd_path is not None:
            if os.path.exists(update_pd_path):
                new_pd = pd.read_hdf(update_pd_path)
                new_pd = new_pd.append([pd.DataFrame([card_info], columns=self.card_db.columns)], ignore_index=True)
            else:
                new_pd = pd.DataFrame([card_info], columns=self.card_db.columns)
            new_pd.drop_duplicates(subset='web_id', keep='last', inplace=True)
            new_pd.to_hdf(update_pd_path, 'df', mode='w', format='table', data_columns=True)
            self.new_card_db = new_pd

    def _parse_word_test(self, text):
        token_info = {
            'type': '',
            'value': None,
            'neg':False,
        }
        res = False
        word_len = 0
        err_msg = None
        neg = False
        if text[0] == '-' or text[0] == '!':
            neg = True
            text = text[1:]
        token_info['neg'] = neg
        for cond_fun, cond_type, word_type, ret_fun in self.parse_word_list:
            res, ret_data, word_len = cond_fun(text)
            if res:
                token_info['type'] = word_type
                for k, v in ret_fun(ret_data).items():
                    token_info[k] = v
                assert 'value' in token_info
                break
        if type(token_info['value']) == int and not(-10000 < token_info['value'] < 10000):
            res = False
            err_msg = 'int_overflow'
        return res, token_info, text[word_len:], err_msg