from datetime import datetime, timedelta
import pandas as pd


def get_dataframe():
    df = pd.read_csv('posts.csv')
    df = df.set_index('id')
    df['date'] = pd.to_datetime(df['date'], format="%d-%m-%Y %H:%M")
    return df


def get_period(days):
    return datetime.today() - timedelta(days=days)


def filter_posts_by_month(df):
    date_month = get_period(days=30)
    df_month = df[df['date'] >= date_month]

    best_month_by_likes = df_month[df_month.likes == df_month.likes.max()]
    best_month_by_reposts = df_month[df_month.reposts == df_month.reposts.max()]
    best_month_by_views = df_month[df_month.views == df_month.views.max()]

    best_month = pd.concat(
        [best_month_by_likes, best_month_by_reposts, best_month_by_views],
        keys=['LIKE', 'REPOSTS', 'VIEWS']
    )
    return best_month


def filter_posts_by_year(df):
    date_year = get_period(days=365)
    df_year = df[df['date'] >= date_year]

    best_year_by_likes = df_year[df_year.likes == df_year.likes.max()]
    best_year_by_reposts = df_year[df_year.reposts == df_year.reposts.max()]
    best_year_by_views = df_year[df_year.views == df_year.views.max()]

    best_year = pd.concat(
        [best_year_by_likes, best_year_by_reposts, best_year_by_views],
        keys=['LIKE', 'REPOSTS', 'VIEWS']
    )
    return best_year


def filter_posts_by_all_time(df):
    best_all_time_by_likes = df[df.likes == df.likes.max()]
    best_all_time_by_reposts = df[df.reposts == df.reposts.max()]
    best_all_time_by_views = df[df.views == df.views.max()]

    best_all = pd.concat(
        [best_all_time_by_likes, best_all_time_by_reposts, best_all_time_by_views],
        keys=['LIKE', 'REPOSTS', 'VIEWS']
    )
    return best_all


def upload_dataframe_to_csv(df, filename):
    df.to_csv(filename)


def pd_files():
    dataframe = get_dataframe()
    upload_dataframe_to_csv(filter_posts_by_month(dataframe), 'best_month.csv')
    upload_dataframe_to_csv(filter_posts_by_year(dataframe), 'best_year.csv')
    upload_dataframe_to_csv(filter_posts_by_all_time(dataframe), 'best_all.csv')
    print('Файлы сформированы')


