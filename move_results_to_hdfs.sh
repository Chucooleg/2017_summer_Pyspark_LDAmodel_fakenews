#! /bin/bash

# local to data setup

mkdir /data/lda_outputs/
mkdir /data/lda_outputs/lda_wordcloud
mkdir /data/lda_outputs/lda_cossim
mkdir /data/lda_outputs/lda_topics

rm /data/lda_outputs/lda_wordcloud/*
rm /data/lda_outputs/lda_cossim/*
rm /data/lda_outputs/lda_topics/*

cp /home/w205/lda_outputs/lda_wordcloud/* /data/lda_outputs/lda_wordcloud
cp /home/w205/lda_outputs/lda_cossim/* /data/lda_outputs/lda_cossim
cp /home/w205/lda_outputs/lda_topics/* /data/lda_outputs/lda_topics

# HDFS setup

# write code to move from local to HDFS
sudo -u hdfs hadoop fs -rm /user/w205/project1/lda_output/lda_wordcloud/*.csv
sudo -u hdfs hadoop fs -rm /user/w205/project1/lda_output/lda_cossim/*.csv
sudo -u hdfs hadoop fs -rm /user/w205/project1/lda_output/lda_topics/*.csv

cd /home/w205/lda_outputs/lda_wordcloud/

sudo -u hdfs hadoop fs -put /data/lda_outputs/lda_wordcloud/*.csv /user/w205/project1/lda_output/lda_wordcloud
sudo -u hdfs hadoop fs -put /data/lda_outputs/lda_cossim/*.csv /user/w205/project1/lda_output/lda_cossim
sudo -u hdfs hadoop fs -put /data/lda_outputs/lda_topics/*.csv /user/w205/project1/lda_output/lda_topics

# List HDFS files
hdfs dfs -ls /user/w205/project1/lda_output/lda_wordcloud
hdfs dfs -ls /user/w205/project1/lda_output/lda_cossim
hdfs dfs -ls /user/w205/project1/lda_output/lda_topics


