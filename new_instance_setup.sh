# mount disk
# start Hadoop

su - w205 

git clone https://Chucooleg@github.com/shredmore/twitter_fakestnews.git

exit # as root
cd twitter_fakestnews/bens_code/copy_from_pi/
. copy_headlines_from_pi.sh
. copy_tweets_from_pi_2.sh


sudo -uhdfs hadoop fs -mkdir /user/w205/project1/lda_output
sudo -uhdfs hadoop fs -mkdir /user/w205/project1/lda_output/lda_wordcloud
sudo -uhdfs hadoop fs -mkdir /user/w205/project1/lda_output/lda_cossim
sudo -uhdfs hadoop fs -mkdir /user/w205/project1/lda_output/lda_topics

su - w205 
cd ~
mkdir lda_output
mkdir lda_output/lda_wordcloud
mkdir lda_output/lda_cossim
mkdir lda_output/lda_topics