# Misalkan kita ingin mencari age (kelompok umur) sering menonton film dengan genre apa

import argparse
import pandas as pd


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--users-path')
    parser.add_argument('--movies-path')
    parser.add_argument('--ratings-path')
    parser.add_argument('--output-path')

    return parser.parse_args()


def main():
    args = parse_arguments()

    users = pd.read_csv(args.users_path, sep='::', header=None, names=['UserID', 'Gender', 'Age', 'Occupation', 'Zip_Code'])
    movies = pd.read_csv(args.movies_path, sep='::', header=None, names=['MovieID', 'Title', 'Genres'], encoding='latin-1')
    ratings = pd.read_csv(args.ratings_path, sep='::', header=None, names=['UserID', 'MovieID', 'Rating', 'Timestamp'])

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