from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

def fetch_news_data():
    # Fetch data from both sources (mock implementation)
    hdfc_news = ["Sample Article 1 for HDFC", "Sample Article 2 for HDFC"]
    tata_motors_news = ["Sample Article 1 for Tata Motors", "Sample Article 2 for Tata Motors"]
    # Combine news data into a dataframe
    return pd.DataFrame({"company": ["HDFC", "HDFC", "Tata Motors", "Tata Motors"], "article": hdfc_news + tata_motors_news})

def clean_data(news_df):
    # Basic text cleaning, remove duplicates, remove special chars
    news_df['cleaned_article'] = news_df['article'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    return news_df.drop_duplicates(subset="cleaned_article")

def sentiment_analysis(news_df):
    # Mock API call for sentiment analysis
    def get_sentiment_score(text):
        # Simulating an API call that returns a random sentiment score
        return round(random.uniform(0, 1), 2)
   
    news_df['sentiment_score'] = news_df['cleaned_article'].apply(get_sentiment_score)
    return news_df

def persist_data(news_df):
    # Example of storing in a CSV (could be replaced with DB/Datastore)
    news_df.to_csv('/path_to_your_storage/sentiment_scores.csv', index=False)

def send_alert(message):
    # Mock function for sending alerts
    print(f"ALERT: {message}")

# DAG configuration
dag = DAG(
    'pipeline_1',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'email_on_failure': True, 'retries': 1},
    description='Pipeline 1: News Scraping and Sentiment Analysis',
    schedule_interval='0 19 * * 1-5',  # Runs every working day at 7:00 PM
    start_date=datetime(2024, 11, 18),
    catchup=False,
)

# Define the tasks
fetch_task = PythonOperator(task_id='fetch_news_data', python_callable=fetch_news_data, dag=dag)
clean_task = PythonOperator(task_id='clean_data', python_callable=clean_data, op_args=[fetch_task.output], dag=dag)
sentiment_task = PythonOperator(task_id='sentiment_analysis', python_callable=sentiment_analysis, op_args=[clean_task.output], dag=dag)
persist_task = PythonOperator(task_id='persist_data', python_callable=persist_data, op_args=[sentiment_task.output], dag=dag)
alert_task = PythonOperator(task_id='send_alert', python_callable=send_alert, op_args=["Pipeline 1 failed"], dag=dag)

# Set task dependencies
fetch_task >> clean_task >> sentiment_task >> persist_task
persist_task >> alert_task


airflow  2:

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def check_pipeline_1_status():
    # Check if Pipeline 1 ran successfully today (mock implementation)
    return True  # Assume Pipeline 1 ran successfully

def mean_age_per_occupation():
    # Load MovieLens data
    # Example with pandas
    df = pd.read_csv('path_to_movie_lens_data/u.user', sep='|')
    mean_age = df.groupby('occupation')['age'].mean()
    return mean_age

def top_rated_movies():
    # Example to find top-rated movies (at least 35 ratings)
    ratings_df = pd.read_csv('path_to_movie_lens_data/u.data', sep='\t', header=None, names=['user', 'movie', 'rating'])
    movie_ratings = ratings_df.groupby('movie').size()
    top_movies = movie_ratings[movie_ratings >= 35].sort_values(ascending=False).head(20)
    return top_movies

def top_genres_by_age_occupation():
    # Find top genres by age and occupation
    df = pd.read_csv('path_to_movie_lens_data/u.item', sep='|', header=None, names=['movie', 'title'] + ['genre'+str(i) for i in range(19)])
    return df

def similar_movies():
    # Example to find similar movies based on ratings (dummy algorithm)
    df = pd.read_csv('path_to_movie_lens_data/u.data', sep='\t', header=None, names=['user', 'movie', 'rating'])
    # Similarity logic here
    return df

# DAG configuration
dag_2 = DAG(
    'pipeline_2',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'email_on_failure': True, 'retries': 1},
    description='Pipeline 2: MovieLens Data Analysis',
    schedule_interval='0 20 * * 1-5',  # Runs every working day at 8:00 PM
    start_date=datetime(2024, 11, 18),
    catchup=False,
)

# Task definitions
check_pipeline_1_task = PythonOperator(task_id='check_pipeline_1_status', python_callable=check_pipeline_1_status, dag=dag_2)
mean_age_task = PythonOperator(task_id='mean_age_per_occupation', python_callable=mean_age_per_occupation, dag=dag_2)
top_movies_task = PythonOperator(task_id='top_rated_movies', python_callable=top_rated_movies, dag=dag_2)
top_genres_task = PythonOperator(task_id='top_genres_by_age_occupation', python_callable=top_genres_by_age_occupation, dag=dag_2)
similar_movies_task = PythonOperator(task_id='similar_movies', python_callable=similar_movies, dag=dag_2)

# Set task dependencies
check_pipeline_1_task >> [mean_age_task, top_movies_task, top_genres_task, similar_movies_task]