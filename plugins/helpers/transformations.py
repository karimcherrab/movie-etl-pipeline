import json
import ast
import os
from datetime import datetime

import numpy as np
import pandas as pd


class MovieDataTransformer:
    def __init__(self, ti):
        self.ti = ti
        pass

    def clean_movie(self, df):
        df_clean = df.copy()
        print(f'budget = {df_clean}')
        df = self._handle_data(df_clean)
        print(f'budget = {df}')

        df = self._handle_missing_values(df)

        print(f'budget = {df["movies"]}')
        return df

    def _handle_data(self, df):
        movies_parsed = df['movies'].apply(ast.literal_eval)
        credits_parsed = df['credits'].apply(ast.literal_eval)
        reviews_parsed = df['reviews'].apply(ast.literal_eval)
        details_movie = movies_parsed.apply(lambda x: x['details'])
        movies_df = pd.json_normalize(details_movie)

        movies_df['release_year'] = pd.to_datetime(
            movies_df['release_date'], errors='coerce'
        ).dt.year

        movies_df = movies_df[[
            'id', 'adult', 'title', 'release_year', "release_date", 'budget', 'revenue', 'status', 'tagline',
            'original_title',
            'runtime', 'popularity', 'vote_average', 'vote_count', 'overview', 'backdrop_path', 'homepage',
            'original_language',
        ]]

        movies_df = movies_df.rename(columns={'id': 'movie_id'})

        def extract_genres(genres):
            if isinstance(genres, list):
                return [g['name'] for g in genres]
            return []

        genres_df = movies_df[['movie_id']].copy()
        genres_df['genres'] = details_movie.apply(
            lambda x: extract_genres(x['genres'])
        )

        movie_genres_df = genres_df.explode('genres')
        movie_genres_df = movie_genres_df.rename(columns={'genres': 'genre'})

        print(movie_genres_df)

        cast_series = credits_parsed.apply(lambda x: x.get('cast', []))

        movie_cast_df = cast_series.explode()
        movie_cast_df = pd.json_normalize(movie_cast_df)

        movie_cast_df['movie_id'] = credits_parsed.apply(
            lambda x: x.get('movie_id')
        ).repeat(cast_series.apply(len)).values

        movie_cast_df = movie_cast_df[[
            'movie_id', 'actor_id', 'name', 'gender',
            'character', 'order', 'popularity'
        ]]

        review_series = reviews_parsed.apply(lambda x: x if isinstance(x, list) else [])

        movie_reviews_df = review_series.explode()

        movie_reviews_df = pd.json_normalize(movie_reviews_df)
        movie_reviews_df = movie_reviews_df.dropna(subset=['review_id'])
        genres_path = "/opt/airflow/data/movie_genres.parquet"
        cast_path = "/opt/airflow/data/movie_cast.parquet"
        reviews_path = "/opt/airflow/data/movie_reviews.parquet"
        self.save_clean_data(movie_genres_df, genres_path, "genres_path")
        self.save_clean_data(movie_cast_df, cast_path, "cast_path")
        self.save_clean_data(movie_reviews_df, reviews_path, "reviews_path")
        return {
            "movies": movies_df,
            "movie_genres": movie_genres_df,
            "movie_cast": movie_cast_df,
            "movie_reviews": movie_reviews_df
        }

    def _handle_missing_values(self, tables):

        movies = tables["movies"]
        movie_genres = tables["movie_genres"]

        movies_with_genres = movies.merge(
            movie_genres,
            on="movie_id",
            how="left"
        )

        movies_with_genres["budget_clean"] = (
            movies_with_genres["budget"]
            .replace(0, np.nan)
        )

        median_budget = (
            movies_with_genres
            .groupby(["release_year", "genre"])["budget_clean"]
            .median()
        )

        def impute_budget(row):
            if pd.isna(row["budget_clean"]):
                return median_budget.get(
                    (row["release_year"], row["genre"]),
                    np.nan
                )
            return row["budget"]

        movies_with_genres["budget_filled"] = (
            movies_with_genres.apply(impute_budget, axis=1)
        )

        movies["budget"] = (
            movies_with_genres
            .groupby("movie_id")["budget_filled"]
            .median()
            .values
        )

        tables["movies"] = movies
        movies_path = "/opt/airflow/data/movies.parquet"
        self.save_clean_data(movies, movies_path, "movies_path")

        return tables

    def _normalize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalisation complète des dates"""
        date_columns = ['release_date']

        for col in date_columns:
            if col in df.columns:

                df[col] = pd.to_datetime(
                    df[col],
                    errors='coerce',
                    format='mixed'
                )

                df[f'{col}_year'] = df[col].dt.year
                df[f'{col}_month'] = df[col].dt.month
                df[f'{col}_day'] = df[col].dt.day
                df[f'{col}_quarter'] = df[col].dt.quarter
                df[f'{col}_dayofweek'] = df[col].dt.dayofweek

                df[f'{col}_is_valid'] = df[col].notnull()

        if 'release_date' in df.columns:
            current_year = datetime.now().year
            df['release_date_is_future'] = df['release_date'] > datetime.now()
            df['release_date_is_ancient'] = df['release_date'].dt.year < 1900

        return df

    def save_clean_data(self, df, path, key_path):

        df.to_parquet(path, index=True)
        self.ti.xcom_push(key=key_path, value=path)

    def cleanup_old_files(directory, days_to_keep=2):
        """Nettoyer les anciens fichiers temporaires"""
        import time
        now = time.time()
        cutoff = now - (days_to_keep * 86400)

        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                file_time = os.path.getmtime(filepath)
                if file_time < cutoff and filename.startswith('movies_cleaned_'):
                    os.remove(filepath)
                    print(f"Removed old file: {filepath}")
