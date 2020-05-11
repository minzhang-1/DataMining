# Foundations and Applications of Data Mining
Created by [Min Zhang](https://github.com/minzhang-1) for course assignments of INF553 from University of Southern California.

### Introduction
Data mining is a foundational piece of the data analytics skill set. At a high level, it allows the analyst to discover patterns in data, and transform it into a usable product. In this repository, we release code for utilizing data mining and machine learning algorithms to analyze very large real world data sets in this course, such as Yelp dataset, real time Twitter data. Emphasis on Map Reduce. 

### Installation

The code has been tested with Python 2.7 or 3.5. You may need to have java 8.0 and install pyspark package.

To check your java version:
```bash
jave --version
```

To install pyspark for Python:
```bash
sudo pip install pyspark
```

### Data

The [data](https://drive.google.com/drive/folders/1quCqmgkxLcBdbRB6ZTKgILQ2dBXLTqUs?usp=sharing) is collected/created by instructor of the course INF553.

### Content

| Topic         | Algorithm    | Content    |
| ------------- |-------------|-------------|
| Data Explore | - | Preprocess and explore Yelp dataset to be familiar with Spark |
| Frequent Itemset | Aprior, SON | Find frequent itemset in Yelp dataset |
| Recommendation System | Min-Hash, LSH | Build item-based, user-based collaborative filtering and content-based recommendation systems |
| Community Detection | Girvan-Newman | Detect communities in graph |
| Clustering | K-Means, BFR | Cluster dataset with various distance measurements |
| Streaming | Bloom Filtering, Flajolet-Martin, Reservoir Sampling | Filtering, counting and sampling streaming data such as Twitter stream data |


