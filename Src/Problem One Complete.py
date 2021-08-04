# Databricks notebook source
# MAGIC     %sh 
# MAGIC     pip install bs4
# MAGIC     pip install --upgrade pip
# MAGIC     python -m bs4.downloader all

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/13bourned@gmail.com/")# you will have to alter the location
#creates rdd from text files
webpagesURLS = sc.textFile("/FileStore/shared_uploads/13bourned@gmail.com/WikipediaUrls.txt")#you will have to alter location

# COMMAND ----------

def lowerCaseStrip(s):
  lowerCaseStriped = s.lower()
  strings = ["\n","\xa0","-","–","—"]
  symbols = '=>?@[\\]^_`{|}~!")*+,./:;<#$%&\'(1234567890'
  for i in symbols:
    lowerCaseStriped = lowerCaseStriped.replace(i, '')#remove characters
  for i in strings:
    lowerCaseStriped = lowerCaseStriped.replace(i, ' ')# if '': results in joining of wordsdo use space for these characters
  return lowerCaseStriped# results in unwanted spaces will need to filter them after

# COMMAND ----------

def splitWords(s):#splits rdd into worsds when parsed in a map function
  return s.split(" ")

# COMMAND ----------

import urllib
from bs4 import BeautifulSoup

def convertUrlToText(s):# converts a url,s, to raw text from website
  #user agent and headers so no HTTPErrors are thrown due to not being permited to access webpage. 
  user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
  headers={'User-Agent':user_agent,}
  #s = s.strip()
  builtRequest = urllib.request.Request(s,None,headers)#building request
  html = (urllib.request.urlopen(builtRequest).read())
  soup = BeautifulSoup(html, features = "html.parser")#https://stackoverflow.com/questions/328356/extracting-text-from-html-file-using-python
  for script in soup(["script", "style"]):#removing script and style items from soup
    script.extract()  
  text = soup.get_text()
  return text

# COMMAND ----------

"""The commented code was used to determine errors
import numpy as np
webpages = webpagesURLS.collect()
webpagesString = ""
for webpage in np.arange(len(webpages)):
  #print (webpages)
  webpagesString += " " + convertUrlToText(webpages[webpage])

webpageRdd = sc.parallelize(webpagesString) 
print("Dataset generated!")
webpageRddStrip = webpageRdd.map(lowerCaseStrip)
list = webpageRddStrip.collect()
print (list)
"""
#create rdd from wikipedia urls which are in text file
webpagesText = webpagesURLS.map(convertUrlToText)#convert url to text
webpagesTextLowerStrip = webpagesText.map(lowerCaseStrip)#remove unwanted symbols & '\n', converts text to lowercase so can easily count
webpagesTextSplitWords = webpagesTextLowerStrip.flatMap(splitWords)# split into words to prepare for counting

#removeing stopwords
stopWords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

webpagesTextSplitWords = webpagesTextSplitWords.filter(lambda x: x not in stopWords)

#webpagesFiltered = webpagesTextSplitWords.filter(lambda x: '' == x)# inefecetive

# COMMAND ----------

#creating a key and value pair, key = word value = count
"""Here is where i ran into my first major problem. The RDD i created for the text stores each text file as an object, however i wish to convert it into one so that i can perform a key,
value pair for each word which has occured in all of the texts. The result initially creates a key value pair for each text not each word. 

origninal:
webpagesTextSplitWords = webpagesTextLowerStrip.map(splitWords)

Soloution:
webpagesTextSplitWords = webpagesTextLowerStrip.flatMap(splitWords)

flatterns the rdd to consist of just words!
"""
wordIntPair = webpagesTextSplitWords.map(lambda word:(word,1))#creates word and int pair

wordCount = wordIntPair.reduceByKey(lambda x, y: x + y)# applies reduce function so that iint repersents count

#wordCountDecending = wordCount.map(lambda x: (x[1],x[0]))inefective

#list = wordCountDecending.collect()
#print (list)

# COMMAND ----------

#outling columns of data frame
columns = ["Word","Word_Count"]
# create data fram so can write to a CSV
dataFrame = wordCount.toDF(columns)
#removeing null char
dataFrame = dataFrame.filter(dataFrame.Word != "")
#show decending order
dataFrameDesc = dataFrame.orderBy('Word_Count', ascending=False)#Affective!
#showing data frame
dataFrameDesc.show(truncate = False)
#display
dataFrameDesc.select("Word","Word_Count").display()
#write to csv
dataFrameDesc.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/13bourned@gmail.com/ProblemOneInitialPerformance Analysis2.csv")# you will have to alter the location

# COMMAND ----------

import unittest

class TestProblemOne(unittest.TestCase):
  
  def test_splitWords(self):
    self.assertEqual(splitWords("hello world"),["hello","world"])
    self.assertEqual(splitWords("helloWord"),["helloWord"])
    self.assertEqual(splitWords("hello_World. this is Databricks"),["hello_World.","this", "is", "Databricks"])
    
  def test_lowerCaseStrip(self):
    self.assertEqual(lowerCaseStrip("HELLO"),"hello")
    self.assertEqual(lowerCaseStrip("22334"),"")
    self.assertEqual(lowerCaseStrip('=>?@[\\]^_`{|}~!")*+,./:;<#$%&\'(1234567890'),"")
    self.assertEqual(lowerCaseStrip('=>?@[\\]^_`{|}~!")*+,.Hello/:;<#$%&\'(1234567890'),"hello")
    self.assertEqual(lowerCaseStrip("hello\nthis\xa0is-a–Test—thanks"),"hello this is a test thanks")
  
  def test_convertUrlToText(self):
    # hard to test a web scraping funtion so i am producing a mock an testing for exspected outcomes
  # inspiration given by https://www.tutorialspoint.com/python_web_scraping/python_web_scraping_testing_with_scrapers.htm
    url = "https://en.wikipedia.org/wiki/Node"
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    headers={'User-Agent':user_agent,}
    builtRequest = urllib.request.Request(url,None,headers)
    html = (urllib.request.urlopen(builtRequest).read())
    soup = BeautifulSoup(html, features = "html.parser")
    
    #test to see if text can be retreived
    self.assertEqual(soup.find('h1').get_text(),"Node")
    
    for script in soup(["script", "style"]):#removing script and style items from soup
      script.extract()  
    
    #test to see if all style and script elements are removed
    i = 0
    for script in soup(["script", "style"]):
      i += 1
    self.assertEqual(i,0)
      
    #test to see see if text can be exstracted still
    self.assertIsNotNone(soup.get_text())
    
if __name__ == "__main__":
    #unittest.main()
    #print("All Tests Passed")
    #haribaskar - https://forums.databricks.com/questions/14435/how-do-i-run-unit-tests-in-a-notebook-via-nosetest.html
    suite = unittest.TestLoader().loadTestsFromTestCase(TestProblemOne)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
