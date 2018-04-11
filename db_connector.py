import os
import pandas as pd

card_db_col = ['web_id', 'name', 'eng_name', 'card_text', 'hero', 'type', 'cost', 'attack', 'health', 'race', 'rarity', 'expansion', 'img_url', 'detail_url']
alias_db_col = ['web_id', 'alias']
hs_keywords = ['은신', '도발', '돌진', '질풍', '빙결', '침묵', '주문 공격력', '차단', '천상의 보호막', '독성',
               '전투의 함성', '전투의 함성:', '죽음의 메아리', '죽음의 메아리:', '면역', '선택', '선택:', '연계',
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
                                '고놈': '고블린 대 노움',
                                '검은바위 산': '검은바위 산',
                                '검바산': '검은바위 산',
                                '탐험가 연맹': '탐험가 연맹',
                                '탐연': '탐험가 연맹',
                                '마녀숲': '마녀숲'}
        self.type_alias = {'주문': '주문',
                           '하수인': '하수인',
                           '무기': '무기',
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
                              '죽메:': '죽음의 메아리:',}
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

    # load DataFrame type database from the path
    def load(self, card_db_path, alias_db_path):
        assert(os.path.exists(card_db_path))
        assert (os.path.exists(alias_db_path))
        self.card_db = pd.read_hdf(card_db_path)
        self.alias_db = pd.read_hdf(alias_db_path)

        #self.mem_db = self._construct_mem_db(self.card_db)
        self.mem_db = self._construct_mem_db(self.card_db.query(self.standard_query_str))
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
        card_texts = []
        card_texts_norm = []
        for idx, row in df.iterrows():
            if row['web_id'] == 'None':
                continue
            keys.append(row['web_id'])
            names.append(row['name'])
            card_texts.append(row['card_text'])
            card_texts_norm.append(self.normalize_text(row['card_text']))
        return {
            'key': keys,
            'name': names,
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
            stat_query['expansion'] = []
            if ('expansion_group' not in stat_query) or ('정규' in stat_query['expansion_group']):
                stat_query['expansion'] += self.standard_filter
            if ('expansion_group' in stat_query) and ('야생' in stat_query['expansion_group']):
                stat_query['expansion'] += self.wild_filter
        if ('race' in stat_query) and ('모두' not in stat_query['race']):
            stat_query['race'].append('모두')

        for k, v_list in stat_query.items():
            if k in card_db_col:
                cur_value_query = []
                if len(v_list) == 0:
                    continue
                for v in v_list:
                    if type(v) == int:
                        cur_value_query.append('%s == %s' % (k , str(v)))
                    else:
                        cur_value_query.append('%s == \"%s\"' % (k , v))
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

    def query_text(self, query_table, text_query):
        text_query = text_query.strip()
        if len(text_query) == 0:
            if query_table is None:
                return pd.DataFrame(columns=card_db_col)
            query_table = query_table.drop_duplicates(subset='web_id', keep='last')
            return query_table

        if query_table is None:
            assert(self.card_db is not None)
            cur_memdb = self.mem_db
            cur_alias_mem_db = self.alias_mem_db
        else:
            cur_memdb = self._construct_mem_db(query_table)
            #joined = pd.merge(self.alias_db, query_table[['web_id']], on='web_id', how='left')
            joined = self.alias_db.join(query_table[['web_id']].set_index('web_id'), how='inner', on='web_id')
            cur_alias_mem_db = self._construct_alias_mem_db(joined)

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
        return query_table

    def query_text_in_card_text(self, query_table, text_query):
        text_query = text_query.strip()
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

        return self._faster_isin(self.card_db, ret_key)

    # parse the text user types and return its stat query and text query
    def parse_query_text(self, text):
        stat_query = {}
        text_query = ''

        split_list = text.strip().split()
        split_list = [w.strip() for w in split_list]

        idx = 0
        is_invalid = False
        while idx < len(split_list):
            word = split_list[idx]
            next_word = None if (idx == len(split_list) - 1) else split_list[idx+1]
            type, value, use_nextword = self._parse_word(split_list, idx)
            if type == 'none':
                is_invalid = True
                break
            if type == 'end_stat':
                idx += 1
                break

            # process the special case if the stat is concatenated
            elif type == 'attackhealth':
                if 'attack' not in stat_query:
                    stat_query['attack'] = [value[0]]
                else:
                    stat_query['attack'].append(value[0])
                if 'health' not in stat_query:
                    stat_query['health'] = [value[1]]
                else:
                    stat_query['health'].append(value[1])
            # the normal case; type should be the database column string
            else:
                if type not in stat_query:
                    stat_query[type] = [value]
                else:
                    stat_query[type].append(value)

            # jump over the next word if the parser consume it already
            idx += (use_nextword + 1)

        # remove space, \', \, in the text query if it exists
        if idx < len(split_list):
            text_query = ''.join(split_list[idx:])
            text_query = self.normalize_text(text_query, cannot_believe=True)

        # Here, if user query include any non-empty text query,
        # the program thinks the whole user query is for the text query
        # This prevents the situation when the first part of the normal text query
        # is the same with the shape of the stat query
        # ex) 퀘스트 중인 모험가 -> {keyword: 퀘스트} "중인 모험가" (X) "퀘스트 중인 모험가" (O)
        if is_invalid:
            stat_query = {}
            text_query = self.normalize_text(text, cannot_believe=True)

        return stat_query, text_query

    def normalize_text(self, text, cannot_believe=False):
        if cannot_believe:
            table = str.maketrans(dict.fromkeys(' \'\",!?<>();/=+-\\|'))
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

    # detect database column and its query value of a word
    # next word is used to determine the pair number stream (for attack health slots)
    # also it returns use_nextword which becomes true if it 'consumes' the next word while parsing the word
    # return 'none', None, False if it is not part of the database column types
    def _parse_word(self, word_list, word_idx):
        word = word_list[word_idx]
        next_words = word_list[word_idx:]

        ret_type = 'none'
        ret_value = None
        use_nextword = 0

        if word in self.hero_alias.keys():
            ret_type = 'hero'
            ret_value = self.hero_alias[word]
        elif (word[-1] == '코' and  word[:-1].isdigit()):
            ret_type = 'cost'
            ret_value = int(word[:-1].strip())
        elif (word[-3:] == '코스트' and  word[:-3].isdigit()):
            ret_type = 'cost'
            ret_value = int(word[:-3].strip())
        elif word.isdigit() and (len(next_words) > 1 is not None and next_words[1].isdigit()):
            attack = int(word)
            health = int(next_words[1])
            ret_type = 'attackhealth'
            ret_value = (attack, health)
            use_nextword = 1
        elif (word[-1] == '공' and word[:-1].isdigit()):
            attack = int(word[:-1])
            ret_type = 'attack'
            ret_value = attack
        elif (word[-3:] == '공격력' and word[:-3].isdigit()):
            attack = int(word[:-3])
            ret_type = 'attack'
            ret_value = attack
        elif (word[-1] == '체' and word[:-1].isdigit()):
            health = int(word[:-1])
            ret_type = 'health'
            ret_value = health
        elif (word[-2:] == '체력' and word[:-2].isdigit()):
            health = int(word[:-2])
            ret_type = 'health'
            ret_value = health
        elif word in hs_races:
            ret_type = 'race'
            ret_value = word
        elif word in hs_expansion_group:
            ret_type = 'expansion_group'
            ret_value = word

        if ret_type != 'none':
            return ret_type, ret_value, use_nextword

        if '/' in word:
            slash_pos = word.index('/')
            if word[:slash_pos].strip().isdigit() and word[slash_pos+1:].strip().isdigit():
                attack = int(word[:slash_pos].strip())
                health = int(word[slash_pos+1:].strip().strip())
                ret_type = 'attackhealth'
                ret_value = (attack, health)

        elif word in self.type_alias.keys():
            ret_type = 'type'
            ret_value = self.type_alias[word]
        elif word in self.rarity_alias.keys():
            ret_type = 'rarity'
            ret_value = self.rarity_alias[word]
        elif word == ';':
            ret_type = 'end_stat'
            ret_value = None

        result = self._parse_from_list(hs_keywords, next_words)
        if result is not None:
            return ('keyword', result[0], result[1])

        result = self._parse_from_list(self.keyword_alias.keys(), next_words)
        if result is not None:
            return ('keyword', result[0], result[1])
        result = self._parse_from_list(self.expansion_alias.keys(), next_words)
        if result is not None:
            return ('expansion', result[0], result[1])

        return ret_type, ret_value, use_nextword

    def _parse_from_list(self, compare_list, next_words):
        found = None
        found_sp = None
        found_flag = False
        for found in compare_list:
            found_sp = found.split(' ')
            if len(next_words) < len(found_sp):
                continue
            compare = next_words[:len(found_sp)]
            found_flag = True
            for c_idx, c in enumerate(compare):
                if c != found_sp[c_idx]:
                    found_flag = False
                    break
            if found_flag:
                break
        if found_flag:
            ret_value = found
            use_nextword = len(found_sp) - 1
            return ret_value, use_nextword
        else:
            return None
