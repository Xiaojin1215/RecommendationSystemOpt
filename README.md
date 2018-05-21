# Recommendation System Optimization
Project of Insight Data Engineering Fellow 2018B

## Table of Contents
1. [Introduction](README.md#the-product)
2. [Data](README.md#data)
3. [Recommendation System Algorithm](README.md#recommendation-system-algorithm)
4. [Spark Tuning](README.md#spark-tuning)
5. [Data Skew Solution](README.md#data-skew-solution)
6. [Pipeline](README.md#pipeline)
7. [Performance](README.md#performance)

## Introduction
The purpose of this project is to build a recommendation system according to the rates what users gave before. The core algorithm was implemented by Spark. 

## Data
The project started with a subset of [Yelp dataset](https://www.yelp.com/dataset). After that, scale up to the whole dataset, which contains 5.2 million review records. Finally, I simulated 4 times of this dataset to train the recommendation system model.

## Recommendation System Algorithm
The recommendation system model used in this project is item-based collaborative filtering, which calculate the similarity matrix between every two items according to the rates given by common users of those two items. 
The formular of similarity matrix calculation shown as follow:

<div align=center><img width="400" height="120" src="https://github.com/Xiaojin1215/RecommendationSystemOpt/blob/master/Slides/img/cf-formular.png"/></div>
With the similartiy matrix, we can predict the rates of a user to an item by using the formular as follow:
<div align=center><img width="360" height="120" src="https://github.com/Xiaojin1215/RecommendationSystemOpt/blob/master/Slides/img/cf-predict-formular.png"/></div>
A simple example is shown as follow to understand the algorithm better:
<div align=center><img width="450" height="250" src="https://github.com/Xiaojin1215/RecommendationSystemOpt/blob/master/Slides/img/cf-img.png"/></div>
## Data Engineering Challenge and Optimization

### Data Preprocessing

Change the user_id and item_id from a String to Integer.

### Spark Tuning
Increase the value of "Spark.memory.fraction", decreased the value of "Spark.memory.storageFraction". Save more memory for shuffle write. 

### Data Skew Solution
Instead of joining the big table "review" with the small table "avg_star" twice, broadcast the "avg_star" first, and saved it as a variable. Using "map-side join" twice to save time. 

## Pipeline
<div align=center><img width="720" height="300" src="https://github.com/Xiaojin1215/RecommendationSystemOpt/blob/master/Slides/img/pipeline.png"/></div>

## Performance
<div align=center><img width="720" height="300" src="https://github.com/Xiaojin1215/RecommendationSystemOpt/blob/master/Slides/img/performance.png"/></div>
## Author
This code challenge was made by Xiaojin(Ruby)Liu. If you have any questions, please feel free ton contact me through email: <xiaojinliumail@gmail.com>

