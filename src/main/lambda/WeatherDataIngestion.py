from __future__ import print_function

import base64
import urllib2
import urllib
import sys
import json
import os
import boto3

from datetime import date, timedelta
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


#Declare these global variables that we'll use for the access and refresh tokens
clientId = "XXX"
clientSecret = "XXX"

ddb = boto3.client('dynamodb')
ml = boto3.client('machinelearning')

#Some contants defining API error handling responses
ErrorInAPI = "Error when making API call that I couldn't handle"

#This makes an API call.  It also catches errors and tries to deal with them
def MakeAPICall(InURL):
  #Start the request
  InURL = InURL + "&client_id="+clientId+"&client_secret="+clientSecret
  req = urllib2.Request(InURL)

  #Fire off the request
  try:
    #Do the request
    response = urllib2.urlopen(req)
    #Read the response
    FullResponse = response.read()

    #Return values
    return True, FullResponse
  #Catch errors, e.g. A 401 error that signifies the need for a new access token
  except urllib2.URLError as e:
    print ("Got this HTTP error: " + str(e.code))
    HTTPErrorMessage = e.read()
    print ("This was in the HTTP error message: " + HTTPErrorMessage)
    #Return that this didn't work, allowing the calling function to handle it
    return False, ErrorInAPI

#Main part of the code
def lambda_handler(event, context):
  FitbitUserID = "ABC"

  #This is the Aeris API
  AerisAPI = "https://api.aerisapi.com/forecasts/closest?p=-37.8136,144.9631&filter=1hr&limit=24"
  
  #Make the Profile API call
  APICallOK, WeatherData = MakeAPICall(AerisAPI)
  table = "WeatherData"
  
  if APICallOK:
    parsed_data = json.loads(WeatherData)
  
    weatherObjs = parsed_data['response'][0]['periods']

    for period in weatherObjs:
      recordDateTime = str(period['validTime'])
      maxTempC = str(period['maxTempC'])
      minTempC = str(period['minTempC'])
      precipMM = str(period['precipMM'])
      humidity = str(period['humidity'])
      uvi = str(period['uvi'])
      pressureMB = str(period['pressureMB'])
      sky = str(period['sky'])
      feelslikeC = str(period['feelslikeC'])
      windDirDEG = str(period['windDirDEG'])
      windGustKPH = str(period['windGustKPH'])
      windSpeedKPH = str(period['windSpeedKPH'])
      weather = str(period['weather'])
      weatherPrimaryCoded = str(period['weatherPrimaryCoded'])
      isDay = str(period['isDay'])

      item = {
        "FitbitUserID": {"S": FitbitUserID},
        "RecordDateTime": {"S": recordDateTime},
        "maxTempC": {"S": maxTempC},
        "minTempC": {"S": minTempC},
        "precipMM": {"S": precipMM},
        "humidity": {"S": humidity},
        "uvi": {"S": uvi},
        "pressureMB": {"S": pressureMB},
        "sky": {"S": sky},
        "feelslikeC": {"S": feelslikeC},
        "windDirDEG": {"S": windDirDEG},
        "windGustKPH": {"S": windGustKPH},
        "windSpeedKPH": {"S": windSpeedKPH},
        "weather": {"S": weather},
        "weatherPrimaryCoded": {"S": weatherPrimaryCoded},
        "isDay": {"S": isDay}
      }
      response = ddb.put_item(TableName = table, Item = item);
      #print("put response: " + str(response))
    pass
 
  else:
     print( ErrorInAPI )

