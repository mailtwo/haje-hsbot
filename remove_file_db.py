import json
import slack
import os
import pandas as pd
file_db_col = ['date', 'file_id']

def main():
    path = 'bot_token.json'
    with open(path, 'r') as f:
        token_data = json.load(f)
        token_id = token_data['token_id']

    wc = slack.WebClient(token_id, timeout=30)
    file_db_path = os.path.join('database', 'file_db.pd')
    file_db = pd.DataFrame([[pd.to_datetime('now'), 'None']], columns=file_db_col)
    file_db = pd.read_hdf(file_db_path)
    for itr, row in file_db.iterrows():
        file_date = row['date']
        file_id = row['file_id']
        if file_id == 'None':
            continue
        try:
            result = wc.files_delete(
                file=file_db.loc[row.name]['file_id']
            )
        except Exception as e:
            pass

if __name__ == '__main__':
    main()