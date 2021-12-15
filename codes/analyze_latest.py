# Misalkan kita ingin mencari age (kelompok umur) sering menonton film dengan genre apa

import argparse
import json
import pandas as pd


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--users-latest-path')
    parser.add_argument('--movies-latest-path')
    parser.add_argument('--ratings-latest-path')
    parser.add_argument('--output-latest-path')

    return parser.parse_args()


def read_latest(latest_path):
    with open(latest_path) as fp:
        return json.load(fp)['path']


def main():
    args = parse_arguments()



    users = pd.read_csv(
        read_latest(args.users_latest_path),
        sep='::',
        header=None,
        names=['UserID', 'Gender', 'Age', 'Occupation', 'Zip_Code']
    )
    movies = pd.read_csv(
        read_latest(args.movies_latest_path),
        sep='::',
        header=None,
        names=['MovieID', 'Title', 'Genres'],
        encoding='latin-1'
    )
    ratings = pd.read_csv(
        read_latest(args.ratings_latest_path),
        sep='::',
        header=None,
        names=['UserID', 'MovieID', 'Rating', 'Timestamp']
    )

    # Ambil genre yang pertama
    movies['Genre'] = movies['Genres'].apply(lambda genres: genres.split('|')[0])

    all_data = ratings \
        .merge(users[['UserID', 'Age']], on='UserID', how='left') \
        .merge(movies[['MovieID', 'Genre']], on='MovieID', how='left')
    all_data['Count'] = 1

    grouped = all_data[['Age', 'Genre', 'Count']] \
        .groupby(['Age', 'Genre']) \
        .count() \
        .reset_index()

    grouped.to_csv(args.output_path, index=None)


if __name__ == '__main__':
    main()