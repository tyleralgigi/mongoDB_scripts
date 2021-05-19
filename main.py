import re
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

def getAllChampions():
    #https://lol.fandom.com/wiki/Special:CargoTables/Champions
    response = site.api('cargoquery', 
        tables='Champions', 
        fields='Name, BE, RP, Attributes, KeyInteger', 
        limit='max', 
        order_by='KeyInteger', 
    )
    return response
 
def updateAllChampions():
    response = getAllChampions()
    champs = json.loads(json.dumps(response["cargoquery"]))

    for champ in champs:
        data = champCollection.find_one({
            "name" : { "$eq" : champ['title']['Name']}
        })
        
        if "&amp;" in champ['title']['Name']:
            index = champ['title']['Name'].find("&amp;")
            onehalf = champ['title']['Name'][:index]
            twohalf = champ['title']['Name'][index+5:]
            champ['title']['Name'] = onehalf + "&" + twohalf
        
        if data is None:
            try:
                print("adding " + champ['title']['Name'])
                url = image(champ['title']['Name'])
                mydict = {'name': champ['title']['Name'], 
                        'url': url, 
                        'BE': champ['title']['BE'], 
                        'RP': champ['title']['RP'], 
                        'Attributes': champ['title']['Attributes'], 
                        'KeyInteger': champ['title']['KeyInteger']}
                champCollection.insert_one(mydict)
                
            except:
                print("An exception occurred")
        else:
       
                print("updating " + champ['title']['Name'])
                url = image(champ['title']['Name'])
                mydict = {'name': champ['title']['Name'], 
                        'url': url, 
                        'BE': champ['title']['BE'], 
                        'RP': champ['title']['RP'], 
                        'Attributes': champ['title']['Attributes'], 
                        'KeyInteger': champ['title']['KeyInteger']}
                champCollection.update_one({'name': champ['title']['Name']}, {"$set" : mydict}, upsert=True)

def image(name):
    url = "https://lol.fandom.com/wiki/File:{}Square.png".format(name)
    response = requests.get(url).content
    tree = html.fromstring(response)
    imageURL = tree.xpath('//*[@id="mw-content-text"]/div[2]/p/a/@href')
    time.sleep(0.1)
    return imageURL[0]

def getChamp(champName):
    data = champCollection.find_one({
        "name" : { "$eq" : champName}
    })
    if data is None:
        response = site.api('cargoquery', 
            tables='Champions', 
            fields='Name, BE, RP, Attributes, KeyInteger', 
            limit='max', 
            where= f"Name LIKE '%{champName}%'"
        )
        if response['cargoquery'] is not None:
            champ = response['cargoquery'][0]['title']
            if "&amp;" in champ['title']['Name']:
                index = champ['title']['Name'].find("&amp;")
                onehalf = champ['title']['Name'][:index]
                twohalf = champ['title']['Name'][index+5:]
                champ['title']['Name'] = onehalf + "&" + twohalf
            try:
                print("adding " + champ['title']['Name'])
                url = image(champ['title']['Name'])
                mydict = {'name': champ['title']['Name'], 
                        'url': url, 
                        'BE': champ['title']['BE'], 
                        'RP': champ['title']['RP'], 
                        'Attributes': champ['title']['Attributes'], 
                        'KeyInteger': champ['title']['KeyInteger']}
                _id = champCollection.insert_one(mydict)
                return str(_id.inserted_id)
            except:
                print("An exception occurred")
    else: return str(data['_id'])

def getTeam(teamName):
    print(teamName)
    data = teamCollection.find_one({
        "name" : { "$eq" : teamName}
    })
    if data is None:
        response = site.api('cargoquery', 
            tables='Teams', 
            fields='Name, Short, Region', 
            limit='1',
            where= f"Name LIKE '%{teamName}%'"
        )
        if response["cargoquery"] != []:
            try:
                mydict = {
                    "image": leaguepedia_parser.get_team_logo(teamName),
                    "name": teamName,
                    "shortName": response["cargoquery"][0]["title"]["Short"],
                    "Region": response["cargoquery"][0]["title"]["Region"],
                    "Players": []
                }
                _id = teamCollection.insert_one(mydict)
                print("created Team " + str(_id.inserted_id))
                print("Update roster ")
                roster = getRoster(teamName)
                updateTeamRoster(roster, teamName)
                return (_id.inserted_id)
            except Exception as e:
                print(e)
                if teamName.find(".") != -1:
                    index = teamName.find(".")
                    teamName = teamName[:index]
                    getTeam(teamName)
    else:
        print("Found team " + str(data["_id"]))
        return data["_id"]
    
def updateTeamRoster(roster, teamName):
    idList = []
    print(teamName)
    for player in roster:
        _id = getPlayer(player["title"]["Player"])
        if _id != None: 
            idList.append(_id)
    myquery = {"name": teamName}
    newvalues = {"$set": { "Players" : idList}}           
    teamCollection.update_one(myquery, newvalues, upsert=True)
        
def getRoster(teamName):
    response = site.api('cargoquery', 
        tables='Players', 
        fields='Player, Team, IsPersonality', 
        limit='max',
        where= f"IsPersonality = 'No' AND Team LIKE '%{teamName}%'"
    )
    return response["cargoquery"]

def getPlayer(name):
    data =  playerCollection.find_one({"summonerName" : { "$eq" : name}})
    if data == None:
        response = site.api('cargoquery', 
            tables = 'Players', 
            fields = 'Player, Image, Name, Team, Role', 
            limit = '1',
            where = f"Player LIKE '%{name}%'"
        )
        if response['cargoquery'] != []:
            mydict = {
                "fullName": unescape(response["cargoquery"][0]["title"]["Name"]),
                "role": response["cargoquery"][0]["title"]["Role"],
                "summonerName": name,
                "image": response["cargoquery"][0]["title"]["Image"],
                "team": response["cargoquery"][0]["title"]["Team"],
            }
            _id = playerCollection.insert_one(mydict)
            print("created player " + str(_id.inserted_id))
            return _id.inserted_id
        print("No player with " + name)
        return None
    else:
        print("Found player " + str(data["_id"]))
        return data["_id"]

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
            return overviewPageArr[1], str(_id.inserted_id), str(response['type']), str(response['DateStart'])
        else:
            return response['OverviewPage'][0:4], str(_id.inserted_id), str(response['type']), str(response['DateStart'])
    else:
        print("Found event " + str(data["_id"]))
        #updating current event
        if leagueData['currentEventId'] != ObjectId(data["_id"]):
            updateCurrentEvent(leagueData['nameAbbr'], str(data["_id"]), response['OverviewPage'])
        if response['type'] == 'League':
            overviewPageArr =  response['OverviewPage'].split("/")
            return overviewPageArr[1], str(data["_id"]), str(response['type']), str(response['DateStart'])
        else:
            return response['OverviewPage'][0:4], str(data["_id"]), str(response['type']), str(response['DateStart'])

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

def season(seasonName, eventId):
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

def split(splitName, seasonId, dateStart):
    data = splitsCollection.find_one({
        "name" : { "$eq" : splitName},
        "seasonId": { "$eq": ObjectId(seasonId)}
    })
    if data is None:
        dictTemp = {
                "name": splitName,
                "seasonId": ObjectId(seasonId),
                'dateStart': dateStart
        }
        
        _id = splitsCollection.insert_one(dictTemp)
        updateCurrentSplit(seasonId, str(_id.inserted_id))
        print("split created " + str(_id.inserted_id))
        return(_id.inserted_id, None)
    else:
        season = seasonsCollection.find_one({"_id": { "$eq": ObjectId(seasonId)}})
        if season["currentSplit"] != ObjectId(str(data['_id'])):
            updateCurrentSplit(seasonId, str(data['_id']))
        print("split found " + str(data['_id']))
        return data['_id']

def updateCurrentSplit(seasonId, _id):
    print("updating current split")
    myquery = {"_id" : { "$eq" : ObjectId(seasonId)}}
    newvalues = {"$set": { "currentSplit" : ObjectId(_id)}}           
    seasonsCollection.update_one(myquery, newvalues, upsert=True)
      
def splitMatches(splitId, OverviewPage):
    response = site.api('cargoquery',
        limit = "max",
        tables = "MatchSchedule",
        fields = "Team1, Team2, Team1Score, Team2Score, BestOf, OverviewPage, MatchId, DateTime_UTC",
        where = "OverviewPage LIKE \'{}\'".format(OverviewPage),
    )
    matches = response['cargoquery']
    matchIds = []
    for match in matches:
        data = matchesCollection.find_one({
            "OverviewPage" : { "$eq" : match['title']['OverviewPage']},
            "MatchId": { "$eq" : match['title']['MatchId']}
        })
        if data is None:
            mydict = {
                "OverviewPage": match['title']['OverviewPage'],
                "MatchId": match['title']['MatchId'],   
                "splitId": splitId,
                "Team1": getTeam(match['title']['Team1']),
                "Team1Score": match['title']['Team1Score'],
                "Team2": getTeam(match['title']['Team2']),
                "Team2Score": match['title']['Team2Score'],
                "BestOf": match['title']['BestOf']
            }
            
            _id = matchesCollection.insert_one(mydict)
            print("Created match " + str(_id.inserted_id))
            matchIds.append(_id.inserted_id)
        else:
            #checking if current season is empty and if it is updating to the current season
            print("Found match " + str(data["_id"]))
            matchIds.append(str(data["_id"]))
    return(matchIds)

def seasonMatches(OverviewPage):
    response = site.api('cargoquery',
        limit = "max",
        tables = "MatchSchedule",
        fields = "Team1, Team2, Team1Score, Team2Score, BestOf, OverviewPage, MatchId, DateTime_UTC",
        where = "OverviewPage LIKE \'{}\'".format(OverviewPage),
    )
    matches = response['cargoquery']
    matchIds = []
    for match in matches:
        data = matchesCollection.find_one({
            "OverviewPage" : { "$eq" : match['title']['OverviewPage']},
            "MatchId": { "$eq" : match['title']['MatchId']}
        })
        if data is None:
            mydict = {
                "OverviewPage": match['title']['OverviewPage'],
                "MatchId": match['title']['MatchId'],   
                "splitId": None,
                "Team1": getTeam(match['title']['Team1']),
                "Team1Score": match['title']['Team1Score'],
                "Team2": getTeam(match['title']['Team2']),
                "Team2Score": match['title']['Team2Score'],
                "BestOf": match['title']['BestOf']
            }
            
            _id = matchesCollection.insert_one(mydict)
            print("Created match " + str(_id.inserted_id))
            matchIds.append(_id.inserted_id)
        else:
            #checking if current season is empty and if it is updating to the current season
            print("Found match " + str(data["_id"]))
            matchIds.append(str(data["_id"]))
    return(matchIds)

def databaseGames(OverviewPage):
    response = matchesCollection.find({"OverviewPage": { "$eq" : OverviewPage}})
    for match in response:
        matchId = match["MatchId"]
        ScoreboardGames_fields = {
            "Team1Bans",
            "Team1Dragons",
            "Team1Barons",
            "Team1Towers",
            "Team1RiftHeralds",
            "Team1Inhibitors",
            "Team2Bans",
            "Team2Dragons",
            "Team2Barons",
            "Team2Towers",
            "Team2RiftHeralds",
            "Team2Inhibitors",
            "VOD", 
            "DateTime_UTC",
            "UniqueGame",
            "Gamename"
        }
                    
        SG_Response = site.api('cargoquery',
                limit = "max",
                tables = "ScoreboardGames",
                fields = ", ".join(ScoreboardGames_fields) + ", MatchId, Team1, Team2, Team1Score, Team2Score, Winner",
                where = f'MatchId = "{matchId}"'
        )
        for game in SG_Response['cargoquery']:
            UniqueGameStr = game['title']['UniqueGame']
            data = gamesCollection.find_one({
                "UniqueGame":{ "$eq" : UniqueGameStr} 
            })
                
            if data is None:
                Team1String = game['title']['Team1']
                Team2String = game['title']['Team2']
                SP_Response_Team1 = site.api('cargoquery',
                        limit = "max",
                        tables = "ScoreboardPlayers",
                        fields = "Name, Champion, Team,Kills,Deaths,Assists,CS, Items,Runes ,KeystoneRune,Role ",
                        where = f'UniqueGame = "{UniqueGameStr}" AND Team = "{Team1String}"'
                )
                SP_Response_Team2 = site.api('cargoquery',
                        limit = "max",
                        tables = "ScoreboardPlayers",
                        fields = "Name, Champion, Team,Kills,Deaths,Assists,CS, Items,Runes ,KeystoneRune,Role",
                        where = f'UniqueGame = "{UniqueGameStr}" AND Team = "{Team2String}"'
                )
                    
                game['title']['Team1Players'] = {}
                game['title']['Team2Players'] = {}
                      
                for i in range(0,len(SP_Response_Team1['cargoquery'])):
                    print(SP_Response_Team1['cargoquery'][i]['title']['Name'])
                    game['title']['Team1Players'][str(i)] = {
                        "playerID": ObjectId(getPlayer(SP_Response_Team1['cargoquery'][i]['title']['Name'])),
                        "Champ":  SP_Response_Team1['cargoquery'][i]['title']['Champion'],
                        "Kills": SP_Response_Team1['cargoquery'][i]['title']['Kills'],
                        "Deaths":SP_Response_Team1['cargoquery'][i]['title']['Deaths'],
                        "Assists":SP_Response_Team1['cargoquery'][i]['title']['Assists'],
                        "CS":SP_Response_Team1['cargoquery'][i]['title']['CS'],
                        "Items":SP_Response_Team1['cargoquery'][i]['title']['Items'],
                        "Runes":SP_Response_Team1['cargoquery'][i]['title']['Runes'],
                        "KeystoneRune":SP_Response_Team1['cargoquery'][i]['title']['KeystoneRune'],
                        "Role":SP_Response_Team1['cargoquery'][i]['title']['Role'],
                    }
                    game['title']['Team2Players'][str(i)] = {
                        "playerID": ObjectId(getPlayer(SP_Response_Team2['cargoquery'][i]['title']['Name'])),
                        "Champ":  SP_Response_Team2['cargoquery'][i]['title']['Champion'],
                        "Kills": SP_Response_Team2['cargoquery'][i]['title']['Kills'],
                        "Deaths":SP_Response_Team2['cargoquery'][i]['title']['Deaths'],
                        "Assists":SP_Response_Team2['cargoquery'][i]['title']['Assists'],
                        "CS":SP_Response_Team2['cargoquery'][i]['title']['CS'],
                        "Items":SP_Response_Team2['cargoquery'][i]['title']['Items'],
                        "Runes":SP_Response_Team2['cargoquery'][i]['title']['Runes'],
                        "KeystoneRune":SP_Response_Team2['cargoquery'][i]['title']['KeystoneRune'],
                        "Role":SP_Response_Team2['cargoquery'][i]['title']['Role'],
                    }
                        
                    game['title']['Team1'] = ObjectId(getTeam(game['title']['Team1']))
                    game['title']['Team2'] = ObjectId(getTeam(game['title']['Team2']))
                
                
                team1Bans = game['title']['Team1Bans']
                team2Bans = game['title']['Team2Bans']
                game['title']['Team1Bans'] = {}
                game['title']['Team2Bans'] = {}
                team1BanArray = team1Bans.split(',')
                team2BanArray = team2Bans.split(',')
                
                for i in range(0, len(team1BanArray)):
                    try:
                        print("1 " + team1BanArray[i])
                        game['title']['Team1Bans'][str(i)] = {
                            "champId": ObjectId(getChamp(team1BanArray[i]))
                        }
                        print("2 " + team2BanArray[i])
                        game['title']['Team2Bans'][str(i)] = {
                            "champId": ObjectId(getChamp(team2BanArray[i]))
                        }
                    except Exception as e:
                        print(e)
                    
                _id = gamesCollection.insert_one(game['title'])
                print("game inserted " + str(_id.inserted_id))
            else: 
                print("game exists")


def main():
    print("main")
    abbr = ["LCK", "LCS", "LPL", "LEC", "MSI", "European_Masters"]
    
    #if document is updated during execuation it will not be read as the updated until the next execution 
    
    for abbr in abbr:
        leagueData = league(abbr)
        seasonName, _id, eventType, dateStart = event(leagueData)
        seasonId, splitName = season(seasonName, _id)
        OverviewPage = leagueData['currentOverviewPage']
        if eventType == "League":
            print("League")
            splitId = split(splitName, seasonId, dateStart)
            splitMatches(splitId, OverviewPage)
        else:
            print("Tournament")
            seasonMatches(OverviewPage)
        databaseGames(OverviewPage)


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


 
 
 