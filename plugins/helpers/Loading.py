import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from pymongo import MongoClient

class MovieDataLoading():
    def __init__(self):
        self.hook = PostgresHook(postgres_conn_id='postgres_local')
        pass

    def insert_dim_genre(self, data):
        self.load_dim_genre(data)

    def insert_dim_movie(self, data):
        self.load_dim_movie(data)

    def insert_dim_date(self, data):
        self.load_dim_date(data)

    def insert_dim_actor(self, data):
        self.load_dim_actor(data)

    def insert_bridge_movie_genre(self, data_movie, data_genres):
        self.load_bridge_movie_genre(data_movie, data_genres)

    def insert_bridge_movie_actor(self, data_movie, data_cast):
        self.load_bridge_movie_actor(data_movie, data_cast)

    def insert_fact_movie(self, data_movie):
        self.load_fact_Movie(data_movie)

    def insert_review(self, data):
        print(data)
        print(type(data))
        mongo_conn = "mongodb+srv://macherrab:cherrab123@movie.qcrwhyy.mongodb.net/movie"

        client = MongoClient(mongo_conn, ssl=True)

        db = client["review_db"]
        collection = db["review"]
        data = data.where(data.notnull(), None)

        records = data.to_dict(orient="records")

        for record in records:
            collection.update_one(
                {"review_id": record["review_id"]},
                {"$setOnInsert": record},
                upsert=True
            )

    def load_dim_movie(self, df_movies):
        """Charger la dimension Movie"""

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        for _, row in df_movies.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Movie (
                    movie_id, title, original_title, status, 
                    tagline, homepage, adult
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (movie_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    original_title = EXCLUDED.original_title,
                    status = EXCLUDED.status,
                    tagline = EXCLUDED.tagline,
                    homepage = EXCLUDED.homepage,
                    adult = EXCLUDED.adult
            """, (
                row['movie_id'], row['title'], row['original_title'],
                row['status'], row['tagline'], row['homepage'], row['adult']
            ))

        conn.commit()
        cursor.close()
        conn.close()

    def load_dim_date(self, df_movies):

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        dates = pd.to_datetime(
            df_movies["release_date"],
            errors="coerce"
        ).dropna().unique()

        print(dates)

        for date_obj in dates:
            date_id = int(date_obj.strftime('%Y%m%d'))

            cursor.execute("""
                INSERT INTO Dim_Date (date_id, full_date, year, month, day, quarter)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING
            """, (
                date_id,
                date_obj.date(),
                date_obj.year,
                date_obj.month,
                date_obj.day,
                ((date_obj.month - 1) // 3) + 1
            ))

        conn.commit()
        cursor.close()
        conn.close()

    def load_dim_genre(self, df_genres):
        print("geeeeenre")
        print(df_genres)
        """Charger la dimension Genre"""
        conn = self.hook.get_conn()
        cursor = conn.cursor()


        print("geeeeenre2")

        all_genres = df_genres["genre"].dropna().unique()

        print(all_genres)
        for genre_name in all_genres:
            cursor.execute("""
                 INSERT INTO Dim_Genre (genre_name)
                 VALUES (%s)
                 ON CONFLICT (genre_name) DO NOTHING
             """, (genre_name,))

        conn.commit()
        cursor.close()
        conn.close()

    def load_dim_actor(self, movie_cast_df):
        """Charger la dimension Actor"""

        conn = self.hook.get_conn()
        cursor = conn.cursor()
        for _, row in movie_cast_df.iterrows():
            cursor.execute("""
                         INSERT INTO Dim_Actor (actor_id, actor_name, gender, popularity)
                         VALUES (%s, %s, %s, %s)
                         ON CONFLICT (actor_id) DO UPDATE SET
                             actor_name = EXCLUDED.actor_name,
                             gender = EXCLUDED.gender,
                             popularity = EXCLUDED.popularity
                     """, (row['actor_id'], row['name'], row['gender'], row['popularity']))

        conn.commit()
        cursor.close()
        conn.close()


    def load_bridge_movie_genre(self, df_movies, df_genres):
        """Charger la table de liaison Movie-Genre avec ID auto-généré"""
        print("Chargement des relations film-genre...")

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        for idx, movie_row in df_movies.iterrows():
            movie_id = movie_row['movie_id']

            all_genres = df_genres.loc[
                df_genres['movie_id'] == movie_id, 'genre'
            ].tolist()
            print(all_genres)
            for genre_name in all_genres:

                cursor.execute("""
                    SELECT genre_id FROM Dim_Genre 
                    WHERE genre_name = %s
                """, (genre_name,))

                result = cursor.fetchone()

                if result:
                    genre_id = result[0]

                    # Insérer la relation dans la table de liaison
                    cursor.execute("""
                        INSERT INTO Bridge_Movie_Genre (movie_id, genre_id)
                        VALUES (%s, %s)
                        ON CONFLICT (movie_id, genre_id) DO NOTHING
                    """, (movie_id, genre_id))
                else:
                    print(f"Genre '{genre_name}' non trouvé pour le film {movie_id}")

        conn.commit()
        cursor.close()
        conn.close()
        print("Chargement des relations terminé.")

    def load_bridge_movie_actor(self, df_movies, movie_cast_df):
        """Charger la table de liaison Movie-Genre avec ID auto-généré"""
        print("Chargement des relations film-genre...")

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        # Pour chaque film
        for idx, movie_row in df_movies.iterrows():
            movie_id = movie_row['movie_id']

            all_actor = (
                movie_cast_df
                .loc[movie_cast_df['movie_id'] == movie_id, ["actor_id", "character", "order"]]
                .drop_duplicates()
                .to_dict(orient='records')
            )
            for actor in all_actor:
                cursor.execute("""
                        INSERT INTO Bridge_Movie_Actor (movie_id, actor_id,character_name,cast_order)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (movie_id, actor_id) DO NOTHING
                    """, (movie_id, actor["actor_id"], actor["character"], actor["order"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("Chargement des relations terminé.")

    def load_fact_Movie(self, df_movies):
        print("started movie")
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        for _, row in df_movies.iterrows():
            date_obj = pd.to_datetime(
                row["release_date"],
                errors="coerce"
            )
            if pd.isna(date_obj):
                continue
            date = date_obj.date()

            cursor.execute("""
                                SELECT date_id FROM Dim_Date 
                                WHERE full_date = %s
                            """, (date,))

            result = cursor.fetchone()

            if result:
                date_id = result[0]
                print("ssssss")
                cursor.execute("""
                                INSERT INTO Fact_Movie (
                                    movie_id, date_id, budget, 
                                    revenue, popularity, vote_average,vote_count,runtime
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (fact_id) DO UPDATE SET
                                    movie_id = EXCLUDED.movie_id,
                                    date_id = EXCLUDED.date_id,
                                    budget = EXCLUDED.budget,
                                    revenue = EXCLUDED.revenue,
                                    popularity = EXCLUDED.popularity,
                                    vote_average = EXCLUDED.vote_average,
                                    vote_count = EXCLUDED.vote_count,
                                    runtime = EXCLUDED.runtime
                            """, (
                    row['movie_id'], date_id,
                    row['budget'], row['revenue'], row['popularity'], row['vote_average'], row['vote_count'],
                    row['runtime']
                ))

        conn.commit()
        cursor.close()
        conn.close()
        print("close fact movie")
