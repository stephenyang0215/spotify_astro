# spotify_etl
 
This project is working on ETL pipeline with Airflow as orchestration tool to demonstrate the usabiliy of Spotify API. <br />
DBT comes into place as transformation tool when the datasets are loaded to snowflake database. <br />
![Project Structure](Project_Structure.png)
Spotify API End points in this project
[Search for Item](doc:https://developer.spotify.com/documentation/web-api/reference/search)<br />
[Get Artist's Top Tracks](doc:https://developer.spotify.com/documentation/web-api/reference/get-an-artists-top-tracks)<br />
[Get Album Tracks](doc:https://developer.spotify.com/documentation/web-api/reference/get-an-albums-tracks)<br />
[Get Recommendations](doc:https://developer.spotify.com/documentation/web-api/reference/get-recommendations)<br />
![DAG](DAG.png)
