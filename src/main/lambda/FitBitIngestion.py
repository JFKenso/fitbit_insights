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


ddb = boto3.client('dynamodb')
ddbService = boto3.resource('dynamodb')

ml = boto3.client('machinelearning')
sns = boto3.client('sns')

#Use this URL to refresh the access token
TokenURL = "https://api.fitbit.com/oauth2/token"

#Some contants defining API error handling responses
TokenRefreshedOK = "Token refreshed OK"
ErrorInAPI = "Error when making API call that I couldn't handle"

#Get the config from the config file.  This is the access and refresh tokens
def GetConfig():
  tableName = "Fitbit_Authorization_Table"

  table = ddbService.Table(tableName)

  configuredUsers = table.scan(
    FilterExpression=Attr('Status').eq("A")
  )

  return configuredUsers['Items']

def GetUserTokens(FitbitUserID):
  tableName = "Fitbit_Authorization_Table"

  table = ddbService.Table(tableName)

  userData = table.scan(
    FilterExpression=Attr('Status').eq("A") & Key('FitbitUserID').eq(FitbitUserID)
  )
  
  at = userData['Items'][0]['Access_Token']
  rt = userData['Items'][0]['Refresh_Token']

  return at, rt
  
  
def WriteConfig(AccToken,RefToken, userCfg):
  table = "Fitbit_Authorization_Table"
  
  newItem = {
      "FitbitUserID": {"S": userCfg['FitbitUserID']},
      "Access_Token": {"S": AccToken},
      "Refresh_Token": {"S": RefToken},
      "Mobile": {"S": userCfg['Mobile']},
      "Token_Type": {"S": "na"},
      "Status": {"S": "A"},
      "ClientID": {"S": userCfg['ClientID']},
      "ClientSecret": {"S": userCfg['ClientSecret']}
    }

  response = ddb.put_item(TableName = table, Item=newItem)


#Make a HTTP POST to get a new
def GetNewAccessToken(userCfg):
  RefToken = userCfg['Refresh_Token']
  OAuthTwoClientID = userCfg['ClientID']
  ClientOrConsumerSecret = userCfg['ClientSecret']

  #Form the data payload
  BodyText = {'grant_type' : 'refresh_token', 'refresh_token' : RefToken}
  #URL Encode it
  BodyURLEncoded = urllib.urlencode(BodyText)
  print("Using this as the body when getting access token >>" + BodyURLEncoded )

  #Start the request
  tokenreq = urllib2.Request(TokenURL,BodyURLEncoded)

  #Add the headers, first we base64 encode the client id and client secret with a : inbetween and create the authorisation header
  tokenreq.add_header('Authorization', 'Basic ' + base64.b64encode(OAuthTwoClientID + ":" + ClientOrConsumerSecret))
  tokenreq.add_header('Content-Type', 'application/x-www-form-urlencoded')

  #Fire off the request
  try:
    tokenresponse = urllib2.urlopen(tokenreq)

    #See what we got back.  If it's this part of  the code it was OK
    FullResponse = tokenresponse.read()

    #Need to pick out the access token and write it to the config file.  Use a JSON manipluation module
    ResponseJSON = json.loads(FullResponse)

    #Read the access token as a string
    NewAccessToken = str(ResponseJSON['access_token'])
    NewRefreshToken = str(ResponseJSON['refresh_token'])
    #Write the access token to the ini file
    WriteConfig(NewAccessToken,NewRefreshToken, userCfg)

    print("New access token output >>> " + FullResponse)
  except urllib2.URLError as e:
    #Gettin to this part of the code means we got an error
    print ("An error was raised when getting the access token.  Need to stop here")
    print (e.code)
    print (e.read())

#This makes an API call.  It also catches errors and tries to deal with them
def MakeAPICall(InURL, userCfg):
  #Start the request
  req = urllib2.Request(InURL)

  #Add the access token in the header
  req.add_header('Authorization', 'Bearer ' + userCfg['Access_Token'])

  print("Calling URI: " + InURL)
  print ("I used this access token " + userCfg['Access_Token'])
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
    #See what the error was
    if (e.code == 401) and (HTTPErrorMessage.find("Access token expired") > 0):
      print("Trying to refresh access token")
      GetNewAccessToken(userCfg)
      return False, TokenRefreshedOK
    #Return that this didn't work, allowing the calling function to handle it
    return False, ErrorInAPI

#Main part of the code

def lambda_handler(event, context):
  today = date.today().strftime('%Y-%m-%d')

  yesterday = date.today() - timedelta(1)
  yesterday_date = yesterday.strftime('%Y-%m-%d')

  tomorrow = date.today() + timedelta(1)
  tomorrow_date = tomorrow.strftime('%Y-%m-%d')


  #This is the Fitbit URL to use for the API call
  FitbitProfileURL = "https://api.fitbit.com/1/user/-/profile.json"
  FitbitHeartrateURL = "https://api.fitbit.com/1/user/-/activities/heart/date/"+today+"/1d/1min/time/00:00/23:59.json"
  FitbitStepsURL = "https://api.fitbit.com/1/user/-/activities/steps/date/"+today+"/1d/15min.json"
  FitbitSleepURL = "https://api.fitbit.com/1.2/user/-/sleep/date/"+tomorrow_date+".json"
  FitbitActivityURL = "https://api.fitbit.com/1/user/-/activities/list.json?beforeDate=today&sort=desc&offset=0&limit=5"
  
  #Get the config
  userConfigs = GetConfig()

  for userCfg in userConfigs:
    try: 
        FitbitUserID = ""
        Access_Token = ""
        Refresh_Token = ""
    
        #Make the Profile API call
        APICallOK, ProfileData = MakeAPICall(FitbitProfileURL, userCfg)
        table = "ProfileData"
  
        if APICallOK:
          parsed_profile = json.loads(ProfileData)
          weight = str(parsed_profile['user']['weight'])
          FitbitUserID = str(parsed_profile['user']['encodedId'])
          age = str(parsed_profile['user']['age'])
          averageDailySteps = str(parsed_profile['user']['averageDailySteps'])
          height = str(parsed_profile['user']['height'])
          localCity = str(parsed_profile['user']['timezone'])
    
          userCfg['firstName'] = str(parsed_profile['user']['firstName'])
  
          #print ("weight: " + weight)
          #print ProfileData
          #print weight
          #print FitbitUserID
    
          item = {
            "FitbitUserID": {"S": FitbitUserID},
            "RecordDate": {"S": yesterday_date},
            "weight": {"N": weight},
            "averageDailySteps": {"N": averageDailySteps},
            "age": {"N": age},
            "height": {"N": height},
            "localCity": {"S": localCity}
          }
    
          response = ddb.put_item(TableName = table, Item = item);
        else:
          if (ProfileData == TokenRefreshedOK):
            print ("Refreshed the access token.  Can go again")
            Access_Token, Refresh_Token = GetUserTokens(userCfg['FitbitUserID'])
            if (FitbitUserID == ""):
                FitbitUserID = userCfg['FitbitUserID']
            
            userCfg['Access_Token'] = Access_Token
            userCfg['Refresh_Token'] = Refresh_Token
          else:
           print( ErrorInAPI )
        
        sns_message = "Running for " + FitbitUserID + " userCfg: " + userCfg['FitbitUserID']
        sns.publish(TopicArn='arn:aws:sns:us-east-1:445802302022:JFeldmanSMSAlert', Message=sns_message)
    
        #Make the Heartrate API call
        APICallOK, HeartRateData = MakeAPICall(FitbitHeartrateURL, userCfg)
        table = "HeartRateData"
        
        if APICallOK:
          parsed_hr = json.loads(HeartRateData)
          intradayObject = parsed_hr['activities-heart-intraday']['dataset']
      
          for struct in intradayObject:
            recordDateTime = str(yesterday_date) + " " + str(struct['time'])
            item = {
              "FitbitUserID": {"S": FitbitUserID},
              "RecordDate": {"S": recordDateTime},
              "heartrate": {"S": str(struct['value'])}
            }
            response = ddb.put_item(TableName = table, Item = item);
            #print("put response: " + str(response))
          pass
   
          #print (HeartRateData)
        else:
          if (HeartRateData == TokenRefreshedOK):
            print ("Refreshed the access token.  Can go again")
            Access_Token, Refresh_Token = GetUserTokens(FitbitUserID)
            userCfg['Access_Token'] = Access_Token
            userCfg['Refresh_Token'] = Refresh_Token
          else:
           print( ErrorInAPI )
        
    
        #Make the Setps API call
        APICallOK, StepsData = MakeAPICall(FitbitStepsURL, userCfg)
        table = "DailyStepsData"
     
        if APICallOK:
          # First record daily steps
          parsed_steps = json.loads(StepsData)
          steps = str(parsed_steps['activities-steps'][0]['value'])
          print("steps: " + steps)
     
          item = {
            "FitbitUserID": {"S": FitbitUserID},
            "RecordDate": {"S": str(yesterday_date)},
            "steps": {"S": steps}
          }
          response = ddb.put_item(TableName = table, Item = item);
      
          # Then iterate through in 15 minute increments
          table = "GranularStepsData"
          intradayObject = parsed_steps['activities-steps-intraday']['dataset']
     
          for struct in intradayObject:
            recordDateTime = str(yesterday_date) + " " + str(struct['time'])
            item = {
              "FitbitUserID": {"S": FitbitUserID},
              "RecordDate": {"S": recordDateTime},
              "steps": {"S": str(struct['value'])}
            }
            response = ddb.put_item(TableName = table, Item = item);
          pass
        
        else:
          if (StepsData == TokenRefreshedOK):
            print ("Refreshed the access token.  Can go again")
            Access_Token, Refresh_Token = GetUserTokens(FitbitUserID)
            userCfg['Access_Token'] = Access_Token
            userCfg['Refresh_Token'] = Refresh_Token
          else:
           print( ErrorInAPI )
        
        
        #Make the Sleep API call
        APICallOK, SleepData = MakeAPICall(FitbitSleepURL, userCfg)
        table = "DailySleepData"
     
        if APICallOK:
          # First record daily steps
          parsed_sleep = json.loads(SleepData)
     
          totalDeep = parsed_sleep['summary']['stages']['deep']
          totalLight = parsed_sleep['summary']['stages']['light']
          totalRem = parsed_sleep['summary']['stages']['rem']
          totalWake = parsed_sleep['summary']['stages']['wake']
          totalMinsAsleep = parsed_sleep['summary']['totalMinutesAsleep']
          totalTimeInBed = parsed_sleep['summary']['totalTimeInBed']
     
          for sleepObj in parsed_sleep['sleep']:
            dateOfSleep = str(sleepObj['dateOfSleep'])
            sleepEfficiency = sleepObj['efficiency']
            wakeup = str(sleepObj['endTime'])
            isMainSleep = str(sleepObj['isMainSleep'])
     
            if(sleepEfficiency < 50):
                sns_message = userCfg['firstName'] + "'s slep last night was awful.  Danger danger danger!!!"
                sns.publish(TopicArn='arn:aws:sns:us-east-1:445802302022:JFeldmanSMSAlert',
                      Message=sns_message)
            elif(sleepEfficiency < 80) and (sleepEfficiency >= 50):
                sns_message = userCfg['firstName'] + "'s slep last night was pretty poor.  Today's mood could be grumpy."
                sns.publish(TopicArn='arn:aws:sns:us-east-1:445802302022:JFeldmanSMSAlert',
                      Message=sns_message)
            else:
                sns_message = userCfg['firstName'] + "'s slep last night was good.  Today's mood should be happy.  If there's something you've been meaning to tell your hubby, today is the day."
                sns.publish(TopicArn='arn:aws:sns:us-east-1:445802302022:JFeldmanSMSAlert',
                      Message=sns_message)
                
            item = {
              "FitbitUserID": {"S": FitbitUserID},
              "RecordDate": {"S": str(dateOfSleep)},
              "totalDeep": {"S": str(totalDeep)},
              "totalLight": {"S": str(totalLight)},
              "totalRem": {"S": str(totalRem)},
              "totalWake": {"S": str(totalWake)},
              "totalMinsAsleep": {"S": str(totalMinsAsleep)},
              "totalTimeInBed": {"S": str(totalTimeInBed)},
              "sleepEfficiency": {"S": str(sleepEfficiency)},
              "wakeup": {"S": str(wakeup)},
              "isMainSleep": {"S": str(isMainSleep)}
            } 
            print("Daily Sleep Data: " + str(item))
            table = "DailySleepData"        

            response = ddb.put_item(TableName = table, Item = item);
     
            table = "DetailedSleepData"
            for sleepSegments in sleepObj['levels']['data']:
              sleepSegmentTime = sleepSegments['dateTime']
              sleepSegmentLevel = sleepSegments['level']
              sleepSegmentSeconds = str(sleepSegments['seconds'])
            
              item = {
                "FitbitUserID": {"S": FitbitUserID},
                "SleepSegmentTime": {"S": sleepSegmentTime},
                "SleepSegmentLevel": {"S": sleepSegmentLevel},
                "SleepSegmentSeconds": {"S": sleepSegmentSeconds}
              } 
              response = ddb.put_item(TableName = table, Item = item);
            pass
          pass
  
        else:
          if (SleepData == TokenRefreshedOK):
            print ("Refreshed the access token.  Can go again")
            Access_Token, Refresh_Token = GetUserTokens(FitbitUserID)
            userCfg['Access_Token'] = Access_Token
            userCfg['Refresh_Token'] = Refresh_Token
          else:
           print( ErrorInAPI )
    except Exception as e:
        if hasattr(e, 'message'):
            print("Unexpected error: " + e.message)
            sns_message = "Unexpected error: " + str(e.message)
        else:
            print("Unexpected error: " + e)
            sns_message = "Unexpected error: " + str(e)
        sns.publish(TopicArn='arn:aws:sns:us-east-1:445802302022:JFeldmanSMSAlert', Message=sns_message)


