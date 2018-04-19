import os
import re
import pandas as pd

card_db_col = ['web_id', 'name', 'eng_name', 'card_text', 'hero', 'type', 'cost', 'attack', 'health', 'race', 'rarity', 'expansion', 'img_url', 'detail_url']
alias_db_col = ['web_id', 'alias']
hs_keywords = ['은신', '도발', '돌진', '질풍', '빙결', '침묵', '주문 공격력', '차단', '천상의 보호막', '독성',
               '전투의 함성', '전투의 함성:', '죽음의 메아리', '죽음의 메아리:', '면역', '선택 -', '선택', '연계', '연계:'
               '과부하', '비밀', '비밀:', '예비 부품', '격려', '격려:', '창시합', '발견', '비취 골렘', '적응', '퀘스트',
               '퀘스트:', '보상', '보상:', '생명력 흡수', '소집', '개전', '속공', '잔상']
hs_races = ['멀록', '악마', '야수', '용족', '토템', '해적', '기계', '정령', '모두']
hs_expansion_group = ['정규', '야생']

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
                                '마녀숲': '마녀숲'}
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
        expansions = []
        for exp_name in self.standard_filter:
            expansions.append('expansion == \"%s\"' % (exp_name, ))
        expansion_query_str = ' | '.join(expansions)
        self.standard_query_str = '( ' + expansion_query_str + ' )'
        expansions = []
        for exp_name in self.wild_filter:
            expansions.append('expansion == \"%s\"' % (exp_name, ))
        expansion_query_str = ' | '.join(expansions)
        self.wild_query_filter = '( ' + expansion_query_str + ' )'

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
            (hs_expansion_group, 'expansion_group',
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
            (hs_keywords, 'keyword',
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
    def load(self, card_db_path, alias_db_path):
        assert(os.path.exists(card_db_path))
        assert (os.path.exists(alias_db_path))
        self.card_db = pd.read_hdf(card_db_path)
        self.alias_db = pd.read_hdf(alias_db_path)

        #self.mem_db = self._construct_mem_db(self.card_db)
        self.mem_db = self._construct_mem_db(self.card_db)
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        self.keyword_db = {}
        for keyword in hs_keywords:
            cur_list = []
            for idx, row in self.card_db.iterrows():
                if keyword in row['card_text']:
                    cur_list.append(row['web_id'])
            self.keyword_db[keyword] = cur_list

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
            if ('expansion_group' in stat_query) and ('정규' in stat_query['expansion_group']):
                expansion += self.standard_filter
            if ('expansion_group' in stat_query) and ('야생' in stat_query['expansion_group']):
                expansion += self.wild_filter
            if len(expansion) > 0:
                stat_query['expansion'] = expansion
        if ('race' in stat_query) and ('모두' not in stat_query['race']):
            stat_query['race'].append('모두')

        for k, t_list in stat_query.items():
            cur_value_query = []
            if k in card_db_col:
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
                        else:
                            continue

                    if type(v) == int:
                        if -10000 < v < 10000:
                            cur_value_query.append('%s(%s %s %s)' % ('not ' if token['neg'] else '', k, op, str(v)))
                    else:
                        cur_value_query.append('%s %s \"%s\"' % (k, op, v))

            elif k == 'attackhealth':
                cur_value_query = []
                for token in t_list:
                    attack, health = token['value']
                    cur_value_query.append('( %s(%s == %s & %s == %s) )' % ('not ' if token['neg'] else '',
                                                                            'attack', attack, 'health', health))

            elif k == 'costrange':
                cur_value_query = []
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'cost', low, 'cost', high))

            elif k == 'attackrange':
                cur_value_query = []
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'attack', low, 'attack', high))

            elif k == 'healthrange':
                cur_value_query = []
                for token in t_list:
                    low, high = token['value']
                    cur_value_query.append('( %s(%s >= %s & %s <= %s) )' % ('not ' if token['neg'] else '',
                                                                            'health', low, 'health', high))

            if len(cur_value_query) > 0:
                query_str.append('(' + (' | '.join(cur_value_query)) + ')')


        # print (stat_query)
        if len(query_str) > 0:
            query_str = ' & '.join(query_str)
            if self.mode == 'debug':
                print (query_str)
            ret = self.card_db.query(query_str)
        else:
            ret = self.card_db

        if 'keyword' in stat_query:
            for each_keyword in stat_query['keyword']:
                card_list = self.keyword_db[each_keyword]
                ret = self._faster_isin(ret, card_list)

        return ret

    def query_text(self, query_table, stat_query, text_query):
        text_query = text_query.strip()
        if len(text_query) == 0:
            if query_table is None:
                return pd.DataFrame(columns=card_db_col), pd.DataFrame(columns=card_db_col), pd.DataFrame(columns=card_db_col)
            query_table = query_table.drop_duplicates(subset='web_id', keep='last')
            std_df = pd.DataFrame(columns=query_table.columns)
            wild_df = pd.DataFrame(columns=query_table.columns)
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
            exactly_match = False
            for idx, each_name in enumerate(name_list):
                if text_query == each_name:
                    ret_key = [cur_memdb['key'][idx]]
                    exactly_match = True
                    break
                elif text_query in each_name:
                    ret_key.append(cur_memdb['key'][idx])

            if not exactly_match:
                name_list = cur_alias_mem_db['name']
                for idx, each_name in enumerate(name_list):
                    if text_query in each_name:
                        ret_key.append(cur_alias_mem_db['web_id'][idx])

            query_table = self._faster_isin(self.card_db, ret_key)
            query_table.drop_duplicates(subset='web_id', keep='last', inplace=True)
            std_df = pd.DataFrame(columns=query_table.columns)
            wild_df = pd.DataFrame(columns=query_table.columns)

        if not query_table.empty:
            if ('expansion_group' not in stat_query) and ('expansion' not in stat_query):
                df_group = query_table.groupby('expansion')
                std_list = [df_group.get_group(x) for x in self.standard_filter if x in df_group.groups]
                wild_list = [df_group.get_group(x) for x in self.wild_filter if x in df_group.groups]\

                if len(std_list) > 0:
                    std_df = pd.concat(std_list)
                if len(wild_list) > 0:
                    wild_df = pd.concat(wild_list)
                query_table = std_df if not std_df.empty else wild_df

        return query_table, std_df, wild_df

    def query_text_in_card_text(self, query_table, stat_query, text_query):
        text_query = text_query.strip()
        if len(text_query) == 0:
            if query_table is None:
                return pd.DataFrame(columns=card_db_col), pd.DataFrame(columns=card_db_col), pd.DataFrame(columns=card_db_col)
            query_table = query_table.drop_duplicates(subset='web_id', keep='last')
            std_df = pd.DataFrame(columns=query_table.columns)
            wild_df = pd.DataFrame(columns=query_table.columns)
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
            std_df = pd.DataFrame(columns=query_table.columns)
            wild_df = pd.DataFrame(columns=query_table.columns)

        if not query_table.empty:
            if ('expansion_group' not in stat_query) and ('expansion' not in stat_query):
                df_group = query_table.groupby('expansion')
                std_list = [df_group.get_group(x) for x in self.standard_filter if x in df_group.groups]
                wild_list = [df_group.get_group(x) for x in self.wild_filter if x in df_group.groups]\

                if len(std_list) > 0:
                    std_df = pd.concat(std_list)
                if len(wild_list) > 0:
                    wild_df = pd.concat(wild_list)
                query_table = std_df if not std_df.empty else wild_df

        return query_table, std_df, wild_df

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

        return stat_query, text_query, None

    def normalize_text(self, text, cannot_believe=False):
        if cannot_believe:
            table = str.maketrans(dict.fromkeys(' \'\",!?<>();/=+\\|'))
            return text.translate(table)
        else:
            table = str.maketrans(dict.fromkeys(' \',:*_'))
            return text.translate(table)

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

    def update_alis(self, alias_id, update_to):
        if alias_id not in self.alias_db.index:
            return 'empty'
        self.alias_db.at[alias_id, 'alias'] = update_to
        self.alias_mem_db = self._construct_alias_mem_db(self.alias_db)
        return 'success'

    def flush_alias_db(self):
        alias_path = os.path.join('database', 'alias.pd')
        self.alias_db.to_hdf(alias_path, 'df', mode='w', format='table', data_columns=True)

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