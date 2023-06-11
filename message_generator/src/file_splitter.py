import pandas as pd


def main():
    df = pd.read_csv('tweets.csv')

    middle = len(df) // 2

    df_part1: pd.DataFrame = df.loc[:middle - 1, :]
    df_part2: pd.DataFrame = df.loc[middle:, :]

    df_part1.to_csv('tweets_part1.csv', index=False)
    df_part2.to_csv('tweets_part2.csv', index=False)


if __name__ == '__main__':
    main()
