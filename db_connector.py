import os
import pandas as pd

class DBConnector(object):
    def __init__(self):
        self.card_db = None
        self.alias_db = None
        self.hero_alias = {
                            '드루이드': '드루이드',
                            '드루': '드루이드',
                            '사냥꾼': '사냥꾼',
                            '냥꾼': '사냥꾼',
                            '마법사': '마법사',
                            '법사': '마법사',
                            '성기사': '성기사',
                            '사제': '사제',
                            '도적': '도적',
                            '주술사': '주술사',
                            '흑마법사': '흑마법사',
                            '전사': '전사',}
        self.expansion_alias = {}
        self.type_alias = {'주문': '주문',
                             '하수인': '하수인',
                             '무기': '무기',
                             '죽음의 기사': '영웅 교체',
                             '죽기': '영웅 교체',}
        self.rarity_alias = {'일반': '일반',
                             '희귀': '희귀',
                             '전설': '전설',
                             '영웅': '영웅',}

    # load DataFrame type database from the path
    def load(self, card_db_path, alias_db_path):
        assert(os.path.exists(card_db_path))
        assert (os.path.exists(alias_db_path))
        self.card_db = pd.read_hdf(card_db_path)
        self.alias_db = pd.read_hdf(alias_db_path)
        # @TODO: fill in self.hero_alias

    def query_info(self, query):
        assert(self.card_db is not None)
        query_str = []
        for k, v in query.items():
            query_str.append('%s == \"%s\"' % (k , v))
        ret = self.card_db.query(' & '.join(query_str))
        return ret

    # parse the text user types and return its stat query and text query
    def parse_query_text(self, text):
        stat_query = {}
        text_query = ''

        split_list = text.strip().split()

        idx = 0
        while idx < len(split_list):
            word = split_list[idx]
            next_word = None if (idx == len(split_list) - 1) else split_list[idx+1]
            type, value, use_nextword = self._parse_word(word, next_word)
            if type == 'none':
                break

            # process the special case if the stat is concatenated
            elif type == 'attackhealth':
                stat_query['attack'] = value[0]
                stat_query['health'] = value[1]
            # the normal case; type should be the database column string
            else:
                stat_query[type] = value

            # jump over the next word if the parser consume it already
            if use_nextword:
                idx += 2
            else:
                idx += 1

        # remove space, \', \, in the text query if it exists
        if idx < len(split_list):
            text_query = ''.join(split_list[idx:])
            text_query = text_query.replace('\'', '')
            text_query = text_query.replace(',', '')

        return stat_query, text_query

    # detect database column and its query value of a word
    # next word is used to determine the pair number stream (for attack health slots)
    # also it returns use_nextword which becomes true if it 'consumes' the next word while parsing the word
    # return 'none', None, False if it is not part of the database column types
    def _parse_word(self, word, next_word):
        word = word.strip()
        if next_word is not None:
            next_word = next_word.strip()

        ret_type = 'none'
        ret_value = None
        use_nextword = False

        if word in self.hero_alias.keys():
            ret_type = 'hero'
            ret_value = self.hero_alias[word]
        elif word[-1] == '코' and  word[:-1].strip().isdigit():
            ret_type = 'cost'
            ret_value = int(word[:-1].strip())
        elif word.isdigit() and (next_word is not None and next_word.isdigit()):
            attack = int(word)
            health = int(next_word)
            ret_type = 'attackhealth'
            ret_value = (attack, health)
            use_nextword = True
        else:
            if '/' in word:
                slash_pos = word.index('/')
                if word[:slash_pos].strip().isdigit() and word[slash_pos+1:].strip().isdigit():
                    attack = int(word[:slash_pos].strip())
                    health = int(word[slash_pos+1:].strip().strip())
                    ret_type = 'attackhealth'
                    ret_value = (attack, health)

            elif word in self.expansion_alias.keys():
                ret_type = 'expansion'
                ret_value = self.expansion_alias[word]

            elif word in self.type_alias.keys():
                ret_type = 'type'
                ret_value = self.type_alias[word]
            elif word in self.rarity_alias.keys():
                ret_type = 'rarity'
                ret_value = self.rarity_alias[word]

        return ret_type, ret_value, use_nextword

