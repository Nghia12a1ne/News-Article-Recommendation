import pandas as pd
import os
from newspaper import Article # pip install newspaper3k

comment_dir = "doisongnew"
new_comment_dir = "doisongnew2"

# create a function add the content column to the dataframe
def add_content_column(row):
    try:
        url = row['URL']
        article = Article(url)
        article.download()
        article.parse()
        row['news_content'] = article.text
        return row
    except:
        return None

for csv_file in os.listdir(comment_dir):
    print('Adding new column to: ', csv_file)
    df = pd.read_csv("{}/{}".format(comment_dir, csv_file))
    df = df.apply(add_content_column, axis=1)
    df.to_csv("{}/{}".format(new_comment_dir, csv_file), index=False)

print('All done')