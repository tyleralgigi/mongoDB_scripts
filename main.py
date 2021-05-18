import pymongo
import mwclient
import requests
import json
from datetime import datetime
import sys
import time
from lxml import html
import leaguepedia_parser
from bson.objectid import ObjectId
from collections import OrderedDict
from html import unescape
import os

#site url
site = mwclient.Site('lol.fandom.com', path='/')

#mongo db connection
client = pymongo.MongoClient("URL_FOR_DATABASE")
db = client['lol']

#date
dt = datetime.today()

champCollection = db['champ']
playerCollection = db['players']
teamCollection = db['teams']
leaguesCollection = db['leagues']
eventsCollection = db['events']
matchesCollection = db['matches']
seasonsCollection = db['seasons']
splitsCollection = db['splits']
gamesCollection = db['games']

def league(name):
    print("here")
    data = leaguesCollection.find_one({
            "nameAbbr" : { "$eq" :name}
    })
    if data is not None: return data
    else: return data

def event(leagueData):
    currentEvent = getCurrentEvent(leagueData['nameAbbr'])
    response = currentEvent['cargoquery'][0]['title']
    data = eventsCollection.find_one({"name" : { "$eq" : response['Name']}})
    if data is None:
        #if first tournament in response does not exist that means its just starting and needs to be added
        #update leagues torunamnetId 
        
        dictTemp = {
            "name": response['Name'],
            "currentSeasonId": "",
            "currentOverviewPage": response['OverviewPage'],
            'type': response['type']
        }
        _id = eventsCollection.insert_one(dictTemp)
        print("creating event " + str(_id.inserted_id))
        
        #updating current event
        updateCurrentEvent(leagueData['nameAbbr'], str(_id.inserted_id), response['OverviewPage'])
        
        if response['type'] == 'League':
            overviewPageArr =  response['OverviewPage'].split("/")
            return overviewPageArr[1], str(_id.inserted_id), str(response['type'])
        else:
            return response['OverviewPage'][0:4], str(_id.inserted_id), str(response['type'])
    else:
        print("Found event " + str(data["_id"]))
        #updating current event
        if leagueData['currentEventId'] != ObjectId(data["_id"]):
            updateCurrentEvent(leagueData['nameAbbr'], str(data["_id"]), response['OverviewPage'])
        if response['type'] == 'League':
            overviewPageArr =  response['OverviewPage'].split("/")
            return overviewPageArr[1], str(data["_id"]), str(response['type'])
        else:
            return response['OverviewPage'][0:4], str(data["_id"]), str(response['type'])

def updateCurrentEvent(nameAbbr, _id, overviewPage):    
    print("updating Current Event")
    myquery = {"nameAbbr" : { "$eq" : nameAbbr}}
    newvalues = {"$set": { "currentEventId" : ObjectId(_id)}}           
    leaguesCollection.update_one(myquery, newvalues, upsert=True)
    newvalues = {"$set": { "currentOverviewPage" : overviewPage}}    
    leaguesCollection.update_one(myquery, newvalues, upsert=True)
    
def getCurrentEvent(name,year=str(dt.year)):
    print("Getting Tournament name from cargo table")
    #whereString = '%{name}/{year}%'.format(name=name,year=year)
    whereString = '%{name}/%'.format(name=name)
    todayDate = datetime.today().strftime('%Y-%m-%d')
    date = '{date}'.format(date=todayDate)
    
    response = site.api('cargoquery',
        limit = '1',
        tables = "Tournaments",
        fields = "OverviewPage, League, DateStart, Split, Name",
        where = "DateStart <= \'{date}\' AND OverviewPage LIKE \'{name}\'".format(date=date, name=whereString),
        order_by="DateStart DESC"
    )
    
    if response['cargoquery'] == []:
        response = site.api('cargoquery',
            limit = '10',
            tables = "Tournaments",
            fields = "OverviewPage, League, DateStart, Split, Name",
            where = f"DateStart <= \'{date}\' AND Name LIKE '%{name}%'",
            order_by="DateStart DESC"
        )
        if response['cargoquery'] != []:
            response["cargoquery"][0]['title']['type'] = "Tournament"
    else:
        if response['cargoquery'] != []:
            response["cargoquery"][0]['title']['type'] = "League"

    return response

def databaseSeason(seasonName, eventId):
    data = seasonsCollection.find_one({
        "name" : { "$eq" : seasonName},
        "eventId": { "$eq": ObjectId(eventId)}
    })
    if data is None:
        event = eventsCollection.find_one({"_id": { "$eq": ObjectId(eventId)}})
        
        if event["type"] == "Tournament":
            
            mydict = {
                "name": seasonName,
                "eventId": ObjectId(eventId),
                "currentSplit": None,
            }
            _id = seasonsCollection.insert_one(mydict)
            
            print("Season created " + str(_id.inserted_id))
            updateCurrentSeason(str(_id.inserted_id), eventId)
            return(_id.inserted_id, None)
        else:
            
            mydict = {
                "name": seasonName,
                "eventId": ObjectId(eventId),
                "currentSplit": "",
            }
            _id = seasonsCollection.insert_one(mydict)
            print("Season created " + str(_id.inserted_id))
            updateCurrentSeason(eventId, str(_id.inserted_id))
            return(_id.inserted_id, event['currentOverviewPage'].split('/')[2])
    else:
        event = eventsCollection.find_one({"_id": { "$eq": ObjectId(eventId)}})
        print("Season found " + str(data["_id"]))
        if event['currentSeasonId'] != ObjectId(data["_id"]):
            updateCurrentSeason(eventId, str(data["_id"]))
        if event['type'] == 'League':
            return(data["_id"], event['currentOverviewPage'].split('/')[2])
        else:
            return(data["_id"], "None")

def updateCurrentSeason(eventId, _id):
    print("updating current season")
    myquery = {"_id" : { "$eq" : ObjectId(eventId)}}
    newvalues = {"$set": { "currentSeasonId" : ObjectId(_id)}}           
    eventsCollection.update_one(myquery, newvalues, upsert=True)
    
    
def main():
    print("main")
    abbr = ["LCK", "LCS", "LPL", "LEC", "MSI", "European_Masters"]
    
    leagueData = league("LCK")
    seasonName, _id, tournType = event(leagueData)
    seasonId, splitName = databaseSeason(seasonName, _id)
    OverviewPage = leagueData['currentOverviewPage']
    if tournType == "League":
        print("League")
        #splitId = databaseSplit(splitName, seasonId, response['cargoquery'][0]['title']['DateStart'])
        #matchIds = databaseSplitMatches(splitId, OverviewPage)
    else:
        print("Tournament")
        #matchIds = databaseSeasonMatches(OverviewPage)
    
main()



#might be needed later

def getNextEvent(name,year=str(dt.year)):
    print("Getting next event from cargo table")
    #whereString = '%{name}/{year}%'.format(name=name,year=year)
    whereString = "%LCK/2021 Season/%"
    todayDate = datetime.today().strftime('%Y-%m-%d')
    date = '{date}'.format(date=todayDate)

    response = site.api('cargoquery',
        limit = '1',
        tables = "Tournaments",
        fields = "OverviewPage, League, DateStart, Split, Name",
        where = "DateStart IS NULL AND OverviewPage LIKE \'{name}\'".format(date=date, name=whereString),
        order_by="DateStart DESC"
    )
    
    print(response)
    
def getPastEvents(name,year=str(dt.year)):
    print("Getting Tournament name from cargo table")
    #whereString = '%{name}/{year}%'.format(name=name,year=year)
    whereString = '%{name}/%'.format(name=name)
    todayDate = datetime.today().strftime('%Y-%m-%d')
    date = '{date}'.format(date=todayDate)
    
    response = site.api('cargoquery',
        limit = 'max',
        tables = "Tournaments",
        fields = "OverviewPage, League, DateStart, Split, Name",
        where = "DateStart <= \'{date}\' AND OverviewPage LIKE \'{name}\'".format(date=date, name=whereString),
        order_by="DateStart DESC"
    )
    
    if response['cargoquery'] == []:
        response = site.api('cargoquery',
            limit = 'max',
            tables = "Tournaments",
            fields = "OverviewPage, League, DateStart, Split, Name",
            where = f"DateStart <= \'{date}\' AND Name LIKE '%{name}%'",
            order_by="DateStart DESC"
        )
        if response['cargoquery'] != []:
            response["cargoquery"][0]['title']['type'] = "Tournament"
    else:
        if response['cargoquery'] != []:
            response["cargoquery"][0]['title']['type'] = "League"
