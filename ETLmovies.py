import pandas as pd
import psycopg2
import numpy as np
import re
from pymongo import MongoClient

con = psycopg2.connect(
database = "myfirstdb",
user ="postgres",
password = "****",
host ="localhost",
port = "5432"
)

def extract():
    top_movies()
    unique_genre()
    count_genre_movies()
    latest_movie()
    oldest_movie()
    count_movie_by_year()
    moviecount_by_ratings()
    userratings_moviecount()
    average_ratings_each_movie()

def unique_genre():
    print("hi5")
    cursor_obj = con.cursor()
    cursor_obj.execute("select distinct(genres) from movies")
    result = cursor_obj.fetchall()
    data = pd.DataFrame(result)
    data1 = data[0].str.split('|', expand=True)
    total_genre = data1[0].unique()
    ds_total_genre = pd.DataFrame(total_genre)
    print(ds_total_genre)
    return total_genre
        #ds_total_genre.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/unique_genre.csv")
        
        
    
def top_movies():
    cursor_obj = con.cursor()
    cursor_obj.execute("select title from movies as m  join (SELECT distinct(movieid),count(movieid) as cou FROM ratings as r  group by movieid  order by cou desc limit 10) rated on rated.movieid = m.movieid")
    result = cursor_obj.fetchall()
    ds_top_movies = pd.DataFrame(result)
    print(ds_top_movies)
    return ds_top_movies

def count_genre_movies():
    r1 =[]
    list1 = unique_genre()
    new_list = [item.replace("'s", '') for item in list1]
    #print(new_list)
    for i in new_list:
        cursor_obj = con.cursor()
        cursor_obj.execute(f"select count(movieid) from movies where genres like '%{i}%'")
        result = cursor_obj.fetchall()
        string1 = str(result)
        string1 = string1.replace("[(","")
        string1 = string1.replace(")]","")
        string1 = string1.replace(",","")
    #print(string1)
    #files_cleaned = [re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", string1) for file in string1]
    #print(string1)
        r1.append([i,string1])
    count_genre_movies = pd.DataFrame(r1)
    return count_genre_movies
    
def movie_year():
    cursor_obj = con.cursor()
    cursor_obj.execute("select distinct(title) from movies")
    result = cursor_obj.fetchall()
    new_string = str(result)
    new_string = new_string.replace("'s","")
    new_string = new_string.replace("(1","|1")
    new_string = new_string.replace("(2","|2")
    new_string = new_string.replace("('","")
    new_string = new_string.replace(")'","")
    new_string = new_string.replace(",)","")
    new_string = new_string.replace("[","")
    new_string = new_string.replace("(","")
    new_string = new_string.replace(")","")
    new_string = new_string.replace('"','')
    new_string = new_string.replace("]","")
    #print(new_string)
    final_list = list(new_string.split(","))
    #print(final_list)
    ds = pd.DataFrame(final_list)
    #display(ds)
    data1 = ds[0].str.split('|', expand=True)
    movie_year= data1.iloc[:, [0,1,]]
    #   df_new1.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/movie_year.csv")
    return movie_year
    
def latest_movie():
    movie_dataframe =movie_year()
    sorted_list = movie_dataframe.sort_values(1,ascending= False)
    new_sorted = sorted_list.head(10)
    latest_movie = pd.DataFrame(new_sorted)
        #latest_movie.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/latest_movie.csv")
    print(new_sorted)
    return new_sorted   

def oldest_movie():
    movie_with_year =movie_year()
    movie_dataframe =pd.DataFrame(movie_with_year)
    sorted_list = movie_dataframe.sort_values(1,ascending= True)
    oldest_movie = sorted_list.head(1)
    oldest_movie = pd.DataFrame(oldest_movie)
        #dfoldest_movie.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/oldest_movie.csv")
    print(oldest_movie)
    return oldest_movie
        

def count_movie_by_year():
    movie_with_year =movie_year()
    movie_dataframe =pd.DataFrame(movie_with_year)
    years_groupby = movie_dataframe.groupby(1)
    counts = years_groupby.size()
    movies_count_by_year = counts.reset_index(name='count')
    dfmovies_countbyyear = pd.DataFrame(movies_count_by_year)
        #dfmovies_countbyyear.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/count_movie_by_year.csv")
    print(dfmovies_countbyyear)
    return dfmovies_countbyyear
        

def moviecount_by_ratings():
    cursor_obj = con.cursor()
    cursor_obj.execute("select distinct(movieid),ratings from ratings")
    result = cursor_obj.fetchall()
    ratings_df = pd.DataFrame(result)
        #display(ratings_df)
    ratings_groupby = ratings_df.groupby(1)
    counts = ratings_groupby.size()
    ratings_count = counts.reset_index(name ='count')
    moviecount_by_ratings = pd.DataFrame(ratings_count)
        #moviecount_by_ratings.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/moviecount_by_ratings.csv")
    print(moviecount_by_ratings)
    return moviecount_by_ratings

def userratings_moviecount():
    cursor_obj = con.cursor()
    cursor_obj.execute("select distinct r.userid,r.movieid,m.title from ratings as r join movies as m on m.movieid = r.movieid")
    result = cursor_obj.fetchall()
    user_rated_movies = pd.DataFrame(result)
        #display(user_rated_movies)
    user_rated_movies_groupby =user_rated_movies.groupby(2)
    counts =user_rated_movies_groupby.size()
    user_rated_movies_counts = counts.reset_index(name ='count')
    userratings_movies_counts = pd.DataFrame(user_rated_movies_counts)
        #userratings_movies_counts.to_csv("C:/Users/ABHISHEK/Desktop/project/transform/userratings_moviecount.csv")
    print(userratings_movies_counts)
    return userratings_movies_counts
        

def average_ratings_each_movie():
    cursor_obj = con.cursor()
    cursor_obj.execute("select m.title,r.ratings from ratings as r join movies as m on m.movieid = r.movieid")
    result = cursor_obj.fetchall()
    user_rated_movies = pd.DataFrame(result)
    #display(user_rated_movies)
    user_rated_movies_groupby =user_rated_movies.groupby(0)
    #display(user_rated_movies_groupby)
    ratings =user_rated_movies_groupby.mean()
    average_ratings_each_movie = pd.DataFrame(ratings)
    print(average_ratings_each_movie)
    return average_ratings_each_movie
        
    


    #print(ds_total_genre,ds_top_movies,count_genre_movies,movie_year,latest_movie,oldest_movie,dfmovies_countbyyear,userratings_movies_counts,moviecount_by_ratings,average_ratings_each_movie)
client = MongoClient("mongodb+srv://abhisunera999:password@cluster0.g6bm2mg.mongodb.net/")
mydb = client["project_movie"]

def loading_genre():
    try:
        movie_genre = client['movie_genre']
        genre = unique_genre()
        collection = pd.DataFrame(genre)
        collection =collection.rename(columns= {0 :'Genre'})
        documents = collection.to_dict(orient='records')
        mydb.movie_genre.insert_many(documents)
        print("Movie genre File Uploaded successfully ")
    except:
        print("Error in loading Movie Genre Documents")

def loading_top_movies():
    try:
        Top_movies = client['Top_movies']
        collection = top_movies()
        collection =collection.rename(columns= {0 :'Movie_Name'})
        documents = collection.to_dict(orient='records')
        mydb.Top_movies.insert_many(documents)
        print("Top 10 movies File Uploaded successfully ")
    except:
        print("Error in loading Top 10 movies file Documents")

def loading_count_genre_movies():
    try:
        count_genre_0f_movies = client['count_genre_of_movies']
        collection = count_genre_movies()
        collection =collection.rename(columns= {0 :'Movie_Name',1:'No_of_movies'})
        documents = collection.to_dict(orient='records')
        mydb.Top_movies.insert_many(documents)
        print("Count genre of movies File Uploaded successfully ")

    except:
        print("Error in loading count genre of movies file Documents")
        

def loading_movie_year():
    try:
        movie_release = client['Movie_release']
        collection = movie_year()
        collection =collection.rename(columns= {0 :'Movie_Name',1:'Year'})
        documents = collection.to_dict(orient='records')
        mydb.movie_release.insert_many(documents)
        print("Movie year File Uploaded successfully ")
    except:
        print("Error in loading movie yea file Documents")

def loading_latest_movie():
    try:
        latest_realeased_movie = client['latest_realeased_movie']
        collection = latest_movie()
        collection =collection.rename(columns= {0 :'Movie_Name',1:'Year'})
        documents = collection.to_dict(orient='records')
        mydb.latest_realeased_movie.insert_many(documents)
        print("Movie year File Uploaded successfully ")
    except:
        print("Error in loading movie yea file Documents")


def loading_oldest_movie():
    try:
        oldest_realesed_movie = client['oldest_realesed_movie']
        collection = oldest_movie()
        collection =collection.rename(columns= {0 :'Movie_Name',1:'Year'})
        documents = collection.to_dict(orient='records')
        mydb.oldest_realesed_movie.insert_many(documents)
        print("Oldest movies sorted File Uploaded successfully ")
    except:
        print("Error in loading oldest movie file Documents")    

def loading_count_movie_by_year():
    try:
        count_movie_year = client['count_movie_year']
        collection = count_movie_by_year()
        collection =collection.rename(columns= {1 :'Movie_Name','count':'Number_of_movie_in that year'})
        documents = collection.to_dict(orient='records')
        mydb.count_movie_year.insert_many(documents)
        print("count_movie_by_year File Uploaded successfully ")
    except:
        print("Error in loading count_movie_by_year file Documents") 

def loading_moviecount_by_ratings():
    try:
        moviecount_by_rating = client['moviecount_by_ratings']
        collection =moviecount_by_ratings()
        collection =collection.rename(columns= {0 :'Movie_Name',1:'Average_Ratings'})
        documents = collection.to_dict(orient='records')
        mydb.moviecount_by_rating.insert_many(documents)
        print("moviecount_by_ratings File Uploaded successfully ")
    except:
        print("Error in loading moviecount_by_ratings file Documents")
    
def loading_user_rated_movies_counts():
    try:
        user_rated_movies_count = client['user_rated_movies_counts']
        collection = userratings_moviecount()
        collection =collection.rename(columns= {2 :'Ratings','count':'Number_of_movies'})
        documents = collection.to_dict(orient='records')
        mydb.user_rated_movies_count.insert_many(documents)
        print("user_rated_movies_counts File Uploaded successfully ")
    except:
        print("Error in loading user_rated_movies_counts file Documents")

def loading_average_ratings_each_movie():
    try:
        ratings_each_movie = client['average_ratings_each_movie']
        collection = average_ratings_each_movie()
        collection =collection.rename(columns= {0 :'Movie_Name', 1:'Average_Rating'})
        documents = collection.to_dict(orient='records')
        mydb.ratings_each_movie.insert_many(documents)
        print("average_ratings_each_movie File Uploaded successfully ")
    except:
        print("Error in loading average_ratings_each_movie file Documents")
    

def load():
    loading_top_movies()
    loading_genre()
    loading_movie_year()
    loading_latest_movie()
    loading_oldest_movie()
    loading_count_movie_by_year()
    loading_moviecount_by_ratings()
    loading_user_rated_movies_counts()
    loading_average_ratings_each_movie()

extract()
load()

