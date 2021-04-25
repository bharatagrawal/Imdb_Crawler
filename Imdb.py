import json
import numpy as np
import requests 
from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
import pyodbc

class IMDB(object):
    
    def __init__(self, url):
        super(IMDB, self).__init__()
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        self.page_dom = etree.HTML(str(soup))
        
    def movie_frame(self):
        movie_frame = self.page_dom.xpath("//div[contains(@class,'lister-item-content')]")
        return movie_frame
    
    def movieData(self):
        movie_name, movie_time,movie_score,movie_votes,movie_gross,movie_links =[],[],[],[],[],[]
        movie_genre,movie_actor,movie_director,movie_creator,movie_description,movie_relase,movie_keywords,movie_ratingCount,movie_ratingValue = [],[],[],[],[],[],[],[],[]
        movie_frame = self.movie_frame()
        for index,movie in enumerate(movie_frame):
            print(index)
            #fetch run time
            
            try: 
                movie_time.append(movie.xpath(".//span[contains(@class,'runtime')]/text()")[0].strip())
            except:
                movie_time.append(np.nan)

            #fetch movie score
            try:
                movie_score.append(movie.xpath(".//span[contains(@class,'metascore ')]/text()")[0].strip())
            except:
                movie_score.append(np.nan)

            # fetch movie votes and Gross 
            movieNumbers = movie.xpath(".//span[contains(@name,'nv')]/text()")
            if len(movieNumbers) == 2:
                movie_votes.append(movieNumbers[0].strip())
                movie_gross.append(movieNumbers[1].strip())
            elif len(movieNumbers) == 1:
                movie_votes.append(movieNumbers[0].strip())
                movie_gross.append(np.nan)
            else:
                movie_votes.append(np.nan)
                movie_gross.append(np.nan)

            #fetch movie link and grep the movie data
            movie_link = movie.xpath("./h3//@href")
            movie_link = "https://www.imdb.com{}".format(movie_link[0])
            movie_links.append(movie_link)
            movie_page = requests.get(movie_link)
            movie_Soup = BeautifulSoup(movie_page.text,'html.parser')
            dom = etree.HTML(str(movie_Soup))  # creating dom for movie page

            movie_data = dom.xpath("//script[contains(@type,'application/ld+json')]/text()")
            movie_data = json.loads(movie_data[0]) #Convert from JSON to Python
            
            #fetch the name of the movie
            try:
                movie_name.append(movie_data["name"].strip())
            except:
                movie_name.append(np.nan)
            #fetch the type of movie.
            try:
                if type(movie_data["genre"]) == str:
                    movie_genre.append(movie_data["genre"])
                else:
                    movie_genre.append(", ".join(movie_data["genre"]))
            except:
                movie_genre.append("")

            # fetch the name of the actors.   
            try:
                movie_actor.append(", ".join(map(lambda x: x["name"], movie_data["actor"])))
            except:
                movie_actor.append(np.nan)

            # fetch the name of the director. 
            try:
                movie_director.append(movie_data["director"]["name"])
            except:
                movie_director.append(np.nan)

            # fetch the name of the creator. 
            try:
                movie_creator.append(", ".join([creator["name"] for creator in movie_data["creator"] if creator["@type"]=="Person"]))
            except:
                movie_creator.append(np.nan)

            # fetch description. 
            try:
                movie_description.append(movie_data["description"])
            except:
                movie_description.append(np.nan)

            # fetch the relase date. 
            try:
                movie_relase.append(movie_data["datePublished"].strip())
            except:
                movie_relase.append(np.nan)

            # fetch the name of the director. 
            try:
                movie_keywords.append(movie_data["keywords"].strip())
            except:
                movie_keywords.append(np.nan)

            # fetch the ratingCount
            try:
                movie_ratingCount.append(int(movie_data["aggregateRating"]["ratingCount"]))
            except:
                movie_ratingCount.append(0)

            # fetch the ratingValue
            try:
                movie_ratingValue.append(float(movie_data["aggregateRating"]["ratingValue"]))
            except:
                movie_ratingValue.append(0.0)
       
        df = pd.DataFrame({"MovieName":movie_name,"Genre":movie_genre, "Run_time":movie_time, "Score":movie_score, "Votes":movie_votes, "Gross":movie_gross, "Url":movie_links, "Actors":movie_actor, "Director":movie_director, "Creators":movie_creator, "Description":movie_description, "Relase":movie_relase, "Keywords":movie_keywords, "RatingCount":movie_ratingCount, "RatingValue":movie_ratingValue})
        return df
    
if __name__ == '__main__':
    data = pd.DataFrame()
    for count in range(2):
        print("Page number: ",count)
        url = "https://www.imdb.com/search/title/?title_type=feature&genres=horror&sort=year,desc&start={}".format(count*50+1)
        site1 = IMDB(url)
        result = site1.movieData()
        data = data.append(result)
    data.reset_index(inplace=True)
    data.to_csv("./data.csv", encoding='utf-8') # store the data into the csv file
    data.to_json("./data.json")  # store the data into the json file
    """
    #change database connectivity
    con = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=192.168.0.1;'
                      'Database=MovieStore;'
                      'uid=user1;'
                      'pwd=user@123;'
                      'Trusted_Connection=no;')
    cur = con.cursor()
    for index,row in data.iterrows():
        cur =cur.execute("insert into MovieDataIMDB(MovieName, Genre, Run_time, Score, Votes, Gross, Url, Actors, Director, Creators, Description, Relase, Keywords, RatingCount, RatingValue) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(row['MovieName'],row['Genre'],row['Run_time'],row['Score'],row['Votes'],row['Gross'],row['Url'],row['Actors'],row['Director'],row['Creators'],row['Description'],row['Relase'],row['Keywords'],row['RatingCount'],row['RatingValue']))
        cur.commit()
    con.close()
    """