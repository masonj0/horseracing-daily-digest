#!/usr/bin/env python3
"""
Unified Racing Report Generator (v14.0 - Final)

This script generates one of two valuable horse racing reports based on the
network environment. It scrapes race data from multiple sources, merges the
results, and performs a time-zone and date-aware analysis.

- Unrestricted Mode: Finds high-value bets by analyzing live odds.
- Restricted Mode: Provides a rich, actionable list of upcoming small-field
  races with deep links to Sky Sports, R&S, Brisnet, and AtTheRaces.
"""

# --- Core Python Libraries ---
import os
import re
import time
import json
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, urljoin

# --- Third-Party Libraries ---
import requests
import pytz
from bs4 import BeautifulSoup

# --- Suppress SSL Warnings ---
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

TIMEZONE_MAP = {
    'UK': 'Europe/London', 'IRE': 'Europe/Dublin', 'FR': 'Europe/Paris',
    'SAF': 'Africa/Johannesburg', 'USA': 'America/New_York', 'CAN': 'America/Toronto',
    'ARG': 'America/Argentina/Buenos_Aires', 'URU': 'America/Montevideo', 'AUS': 'Australia/Sydney',
}

COURSE_TO_COUNTRY_MAP = {
    # USA
    'oaklawn': 'USA', 'arizona downs': 'USA', 'rillito': 'USA', 'turf paradise': 'USA', 'cal expo': 'USA',
    'del mar': 'USA', 'ferndale': 'USA', 'fresno': 'USA', 'golden gate fields': 'USA',
    'golden state racing at pleasanton': 'USA', 'los alamitos quarter horse': 'USA', 'los alamitos thoroughbred': 'USA',
    'pleasanton': 'USA', 'sacramento': 'USA', 'santa anita': 'USA', 'santa rosa': 'USA',
    'arapahoe': 'USA', 'delaware': 'USA', 'dover downs': 'USA', 'harrington raceway': 'USA',
    'gulfstream': 'USA', 'gulfstream simulcast': 'USA', 'pompano': 'USA', 'tampa bay downs': 'USA',
    'prairie meadows': 'USA', 'prairie meadows night': 'USA', 'du quoin state fair': 'USA',
    'fairmount': 'USA', 'hawthorne': 'USA', 'hawthorne matinee': 'USA', 'illinois state fair': 'USA',
    'hoosier': 'USA', 'horseshoe indianapolis': 'USA', 'churchill downs': 'USA', 'cumberland run': 'USA',
    'ellis': 'USA', 'keeneland': 'USA', 'kentucky downs': 'USA', 'oak grove racing': 'USA',
    'sandy ridge': 'USA', 'the red mile': 'USA', 'turfway': 'USA', 'delta downs': 'USA',
    'evangeline downs': 'USA', 'fair grounds': 'USA', 'louisiana downs': 'USA', 'plainridge': 'USA',
    'suffolk downs': 'USA', 'laurel': 'USA', 'laurel simulcast': 'USA', 'ocean downs': 'USA',
    'pimlico': 'USA', 'rosecroft raceway': 'USA', 'timonium': 'USA', 'bangor raceway': 'USA',
    'cumberland': 'USA', 'hazel': 'USA', 'canterbury': 'USA', 'running aces': 'USA', 'fonner': 'USA',
    'far hills': 'USA', 'freehold raceway': 'USA', 'meadowlands': 'USA', 'monmouth at far hills': 'USA',
    'monmouth at meadowlands': 'USA', 'monmouth': 'USA', 'downs at albuquerque': 'USA', 'ruidoso downs': 'USA',
    'sunland': 'USA', 'sunray': 'USA', 'zia': 'USA', 'aqueduct': 'USA', 'batavia downs': 'USA',
    'belmont at saratoga': 'USA', 'buffalo raceway': 'USA', 'finger lakes': 'USA', 'monticello raceway': 'USA',
    'saratoga harness': 'USA', 'saratoga race course': 'USA', 'tioga downs': 'USA', 'vernon downs': 'USA',
    'yonkers raceway': 'USA', 'belterra': 'USA', 'dayton raceway': 'USA', 'delaware co fair': 'USA',
    'delaware county fair': 'USA', 'mahoning valley': 'USA', 'miami valley': 'USA', 'northfield': 'USA',
    'scioto downs': 'USA', 'thistledown': 'USA', 'fair meadows': 'USA', 'remington': 'USA',
    'will rogers downs': 'USA', 'crooked river': 'USA', 'grants pass': 'USA', 'harrahs philadelphia': 'USA',
    'parx racing': 'USA', 'penn national': 'USA', 'pocono downs': 'USA', 'presque isle downs': 'USA',
    'the meadows': 'USA', 'lone star': 'USA', 'retama': 'USA', 'sam houston race': 'USA',
    'colonial downs': 'USA', 'shenandoah downs': 'USA', 'emerald downs': 'USA', 'charles town': 'USA',
    'mountaineer': 'USA', 'wyoming downs': 'USA',
    # International
    'ae - abu dhabi': 'UAE', 'ae - al ain': 'UAE', 'ae - jebel ali': 'UAE', 'ae - meydan': 'UAE',
    'ar - gran premio la': 'ARG', 'ar - hipodromo palermo': 'ARG',
    'au - albany': 'AUS', 'au - albury': 'AUS', 'au - alice springs': 'AUS', 'au - ararat': 'AUS',
    'au - armidale': 'AUS', 'au - ascot': 'AUS', 'au - atherton': 'AUS', 'au - avoca': 'AUS',
    'au - bairnsdale': 'AUS', 'au - balaklava': 'AUS', 'au - ballarat': 'AUS', 'au - ballina': 'AUS',
    'au - balranald': 'AUS', 'au - barcaldine': 'AUS', 'au - bathurst': 'AUS', 'au - beaudesert': 'AUS',
    'au - beaumont': 'AUS', 'au - belmont': 'AUS', 'au - benalla': 'AUS', 'au - bendigo': 'AUS',
    'au - berrigan': 'AUS', 'au - birdsville': 'AUS', 'au - blackall': 'AUS', 'au - bordertown': 'AUS',
    'au - bowen': 'AUS', 'au - bowraville': 'AUS', 'au - broken hill': 'AUS', 'au - broome': 'AUS',
    'au - bunbury': 'AUS', 'au - bundaberg': 'AUS', 'au - cairns': 'AUS', 'au - camperdown': 'AUS',
    'au - canberra': 'AUS', 'au - canterbury': 'AUS', 'au - carnarvon': 'AUS', 'au - casino': 'AUS',
    'au - casterton': 'AUS', 'au - caulfield': 'AUS', 'au - cessnock': 'AUS', 'au - charleville': 'AUS',
    'au - chinchilla': 'AUS', 'au - clare': 'AUS', 'au - cloncurry': 'AUS', 'au - coffs harbour': 'AUS',
    'au - colac': 'AUS', 'au - coleraine': 'AUS', 'au - collie': 'AUS', 'au - coonabarabran': 'AUS',
    'au - coonamble': 'AUS', 'au - cootamundra': 'AUS', 'au - corowa': 'AUS', 'au - cowra': 'AUS',
    'au - cranbourne': 'AUS', 'au - dalby': 'AUS', 'au - darwin': 'AUS', 'au - deagon': 'AUS',
    'au - deloraine': 'AUS', 'au - deniliquin': 'AUS', 'au - derby': 'AUS', 'au - devonport': 'AUS',
    'au - donald': 'AUS', 'au - dongara': 'AUS', 'au - doomben': 'AUS', 'au - dubbo': 'AUS',
    'au - dunkeld': 'AUS', 'au - eagle farm': 'AUS', 'au - echuca': 'AUS', 'au - edenhope': 'AUS',
    'au - emerald': 'AUS', 'au - esk': 'AUS', 'au - esperance': 'AUS', 'au - ewan': 'AUS',
    'au - flemington': 'AUS', 'au - flemington mcc': 'AUS', 'au - forbes': 'AUS', 'au - gatton': 'AUS',
    'au - gawler': 'AUS', 'au - geelong': 'AUS', 'au - geraldton': 'AUS', 'au - gilgandra': 'AUS',
    'au - glen innes': 'AUS', 'au - gold coast': 'AUS', 'au - goondiwindi': 'AUS', 'au - gosford': 'AUS',
    'au - goulburn': 'AUS', 'au - grafton': 'AUS', 'au - great western': 'AUS', 'au - grenfell': 'AUS',
    'au - griffith': 'AUS', 'au - gulgong': 'AUS', 'au - gundagai': 'AUS', 'au - gunnedah': 'AUS',
    'au - gympie': 'AUS', 'au - hamilton': 'AUS', 'au - hanging rock': 'AUS', 'au - hawkesbury': 'AUS',
    'au - hobart': 'AUS', 'au - home hill': 'AUS', 'au - horsham': 'AUS', 'au - innisfail': 'AUS',
    'au - inverell': 'AUS', 'au - ipswich': 'AUS', 'au - kalgoorlie': 'AUS', 'au - katherine': 'AUS',
    'au - kembla grange': 'AUS', 'au - kempsey': 'AUS', 'au - kerang': 'AUS', 'au - kilcoy': 'AUS',
    'au - kilmore': 'AUS', 'au - kingscote': 'AUS', 'au - kununurra': 'AUS', 'au - kyneton': 'AUS',
    'au - launceston': 'AUS', 'au - laverton': 'AUS', 'au - leeton': 'AUS', 'au - leinster': 'AUS',
    'au - leonora': 'AUS', 'au - lismore': 'AUS', 'au - longford': 'AUS', 'au - longreach': 'AUS',
    'au - mackay': 'AUS', 'au - marble bar': 'AUS', 'au - meekatharra': 'AUS', 'au - mildura': 'AUS',
    'au - mindarie': 'AUS', 'au - mingenew': 'AUS', 'au - moe': 'AUS', 'au - moonee valley': 'AUS',
    'au - moora': 'AUS', 'au - moree': 'AUS', 'au - mornington': 'AUS', 'au - morphettville': 'AUS',
    'au - morphettville s': 'AUS', 'au - mortlake': 'AUS', 'au - moruya': 'AUS',
    'au - mount barker': 'AUS', 'au - mount gambier': 'AUS', 'au - mount magnet': 'AUS', 'au - mowbray': 'AUS',
    'au - mt isa': 'AUS', 'au - mudgee': 'AUS', 'au - murray bridge': 'AUS', 'au - murtoa': 'AUS',
    'au - murwillumbah': 'AUS', 'au - muswellbrook': 'AUS', 'au - nanango': 'AUS', 'au - naracoorte': 'AUS',
    'au - narrandera': 'AUS', 'au - narrogin': 'AUS', 'au - narromine': 'AUS', 'au - newcastle': 'AUS',
    'au - newman': 'AUS', 'au - nhill': 'AUS', 'au - norseman': 'AUS', 'au - northam': 'AUS',
    'au - nowra': 'AUS', 'au - nth territory': 'AUS', 'au - oakbank': 'AUS', 'au - orange': 'AUS',
    'au - pakenham': 'AUS', 'au - parkes': 'AUS', 'au - penola': 'AUS', 'au - pingrup': 'AUS',
    'au - pinjarra scarpside': 'AUS', 'au - port augusta': 'AUS', 'au - port hedland': 'AUS',
    'au - port lincoln': 'AUS', 'au - port macquarie': 'AUS', 'au - queanbeyan': 'AUS',
    'au - quirindi': 'AUS', 'au - randwick': 'AUS', 'au - randwick kensington': 'AUS', 'au - rockhampton': 'AUS',
    'au - roebourne': 'AUS', 'au - roma': 'AUS', 'au - rosehill': 'AUS', 'au - sale': 'AUS',
    'au - sandown hillside': 'AUS', 'au - sandown lakeside': 'AUS', 'au - sapphire coast': 'AUS',
    'au - scone': 'AUS', 'au - seymour': 'AUS', 'au - stawell': 'AUS', 'au - stony creek': 'AUS',
    'au - strathalbyn': 'AUS', 'au - sunshine coast': 'AUS', 'au - swan hill': 'AUS', 'au - tamworth': 'AUS',
    'au - taree': 'AUS', 'au - tatura': 'AUS', 'au - tennant creek': 'AUS', 'au - terang': 'AUS',
    'au - thangool': 'AUS', 'au - the valley': 'AUS', 'au - toodyay': 'AUS', 'au - toowoomba': 'AUS',
    'au - townsville': 'AUS', 'au - traralgon': 'AUS', 'au - tumut': 'AUS', 'au - tuncurry': 'AUS',
    'au - wagga': 'AUS', 'au - wagga riverside': 'AUS', 'au - walcha': 'AUS', 'au - wangaratta': 'AUS',
    'au - warracknabeal': 'AUS', 'au - warren': 'AUS', 'au - warrnambool': 'AUS', 'au - warwick': 'AUS',
    'au - warwick farm': 'AUS', 'au - wauchope': 'AUS', 'au - wellington': 'AUS', 'au - werribee': 'AUS',
    'au - wodonga': 'AUS', 'au - wyong': 'AUS', 'au - yalgoo': 'AUS', 'au - yarra valley': 'AUS',
    'au - yeppoon': 'AUS', 'au - york': 'AUS', 'bh - bahrain sakhir': 'BH',
    'ajax downs': 'CAN', 'assiniboia downs': 'CAN', 'century downs': 'CAN', 'century mile': 'CAN',
    'clinton raceway': 'CAN', 'evergreen': 'CAN', 'flamboro downs': 'CAN', 'fort erie': 'CAN',
    'fraser downs': 'CAN', 'georgian downs': 'CAN', 'grand river': 'CAN', 'hanover raceway': 'CAN',
    'hastings': 'CAN', 'hippodrome 3r': 'CAN', 'hippodrome gatineau': 'CAN', 'mohawk raceway': 'CAN',
    'rideau carleton': 'CAN', 'rocky mountain turf club': 'CAN', 'the track on 2': 'CAN',
    'western fair raceway': 'CAN', 'woodbine': 'CAN', 'woodbine mohawk': 'CAN',
    'cl - hipodromo chile': 'CL',
    'de - bad doberan': 'DE', 'de - bad harzburg': 'DE', 'de - baden-baden': 'DE', 'de - bedburg-hau': 'DE',
    'de - berlin': 'DE', 'de - berlin mariendorf': 'DE', 'de - berlin-karlshorst': 'DE', 'de - billigheim': 'DE',
    'de - bremen': 'DE', 'de - cologne': 'DE', 'de - cuxhaven': 'DE', 'de - dinslaken': 'DE',
    'de - dortmund': 'DE', 'de - drensteinfurt': 'DE', 'de - dresden': 'DE', 'de - dusseldorf': 'DE',
    'de - erbach': 'DE', 'de - gelsenkirchen': 'DE', 'de - halle': 'DE', 'de - hamburg': 'DE',
    'de - hamburg bahrenfeld': 'DE', 'de - hamburg-horn': 'DE', 'de - hannover': 'DE', 'de - hassloch': 'DE',
    'de - honzrath': 'DE', 'de - hooksiel': 'DE', 'de - hoppegarten': 'DE', 'de - karlsruhe': 'DE',
    'de - krefeld': 'DE', 'de - lebach': 'DE', 'de - leipzig': 'DE', 'de - magdeburg': 'DE',
    'de - mannheim': 'DE', 'de - meissenheim': 'DE', 'de - miesau': 'DE', 'de - moenchengladbach': 'DE',
    'de - muelheim': 'DE', 'de - munich': 'DE', 'de - munich-daglfing': 'DE', 'de - neuss': 'DE',
    'de - pfarrkirchen': 'DE', 'de - quakenbrueck': 'DE', 'de - rastede': 'DE', 'de - saarbruecken': 'DE',
    'de - sonsbeck': 'DE', 'de - stove': 'DE', 'de - straubing': 'DE', 'de - verden': 'DE', 'de - zweibrucken': 'DE',
    'dk - aalborg': 'DK', 'dk - aalborg galopp': 'DK', 'dk - arhus': 'DK', 'dk - arhus galopp': 'DK',
    'dk - charlottenlund': 'DK', 'dk - klampenborg': 'DK', 'dk - odense': 'DK', 'dk - odense galopp': 'DK', 'dk - skive': 'DK',
    'fi - abo': 'FI', 'fi - forssa': 'FI', 'fi - joensuu': 'FI', 'fi - jyvaskyla': 'FI', 'fi - kaustinen': 'FI',
    'fi - kouvola': 'FI', 'fi - kuopio': 'FI', 'fi - lappeenranta': 'FI', 'fi - loviisa': 'FI', 'fi - mikkeli': 'FI',
    'fi - pori': 'FI', 'fi - rovaniemi': 'FI', 'fi - seinajoki': 'FI', 'fi - tornio': 'FI', 'fi - uleaborg': 'FI',
    'fi - vaasa': 'FI', 'fi - vermo': 'FI', 'fi - ylivieska': 'FI',
    'fr - agen le garenne': 'FR', 'fr - aix les bains': 'FR', 'fr - amiens': 'FR', 'fr - angers': 'FR',
    'fr - argentan': 'FR', 'fr - auteuil': 'FR', 'fr - beaumont de lomagne': 'FR', 'fr - bordeaux': 'FR',
    'fr - cabourg': 'FR', 'fr - caen': 'FR', 'fr - cagnes-sur-mer': 'FR', 'fr - cavaillon': 'FR',
    'fr - chantilly': 'FR', 'fr - chartres': 'FR', 'fr - chateaubriant': 'FR', 'fr - chatelaillon': 'FR',
    'fr - cherbourg': 'FR', 'fr - cholet': 'FR', 'fr - clairefontaine': 'FR', 'fr - compiegne': 'FR',
    'fr - cordemais': 'FR', 'fr - craon': 'FR', 'fr - dax': 'FR', 'fr - deauville': 'FR', 'fr - dieppe': 'FR',
    'fr - divonne': 'FR', 'fr - enghien': 'FR', 'fr - evreux': 'FR', 'fr - feurs': 'FR', 'fr - fontainebleau': 'FR',
    'fr - graignes': 'FR', 'fr - hyeres': 'FR', 'fr - la capelle': 'FR', 'fr - la teste': 'FR',
    'fr - langon-libourne': 'FR', 'fr - laval': 'FR', 'fr - le croise laroche': 'FR', 'fr - le lion d-angers': 'FR',
    'fr - le mans': 'FR', 'fr - le mont saint michel': 'FR', 'fr - le touquet': 'FR', 'fr - les sables d-olonne': 'FR',
    'fr - lisieux': 'FR', 'fr - lyon la soie': 'FR', 'fr - lyon-parilly': 'FR', 'fr - maisons-laffitte': 'FR',
    'fr - marseille vivaux': 'FR', 'fr - marseille-borely': 'FR', 'fr - martinique': 'FR', 'fr - mauquenchy': 'FR',
    'fr - maure de bretagne': 'FR', 'fr - meslay du maine': 'FR', 'fr - mont de marsan': 'FR', 'fr - moulins': 'FR',
    'fr - nancy-brabois': 'FR', 'fr - nantes': 'FR', 'fr - parislongchamp': 'FR', 'fr - pau': 'FR', 'fr - pontchateau': 'FR',
    'fr - pornichet': 'FR', 'fr - rambouillet': 'FR', 'fr - reims': 'FR', 'fr - saint brieuc': 'FR',
    'fr - saint galmier': 'FR', 'fr - saint malo': 'FR', 'fr - saint-cloud': 'FR', 'fr - salon de provence': 'FR',
    'fr - senonnes': 'FR', 'fr - strasbourg': 'FR', 'fr - tarbes': 'FR', 'fr - toulouse': 'FR', 'fr - vichy': 'FR',
    'fr - vincennes': 'FR', 'fr - vire': 'FR', 'fr - vittel': 'FR', 'fr - wissembourg': 'FR',
    'gb - aintree': 'UK', 'gb - ascot': 'UK', 'gb - ayr': 'UK', 'gb - bangor-on-dee': 'UK', 'gb - bath': 'UK',
    'gb - beverley': 'UK', 'gb - brighton': 'UK', 'gb - carlisle': 'UK', 'gb - cartmel': 'UK',
    'gb - catterick bridge': 'UK', 'gb - chelmsford city': 'UK', 'gb - cheltenham': 'UK', 'gb - chepstow': 'UK',
    'gb - chester': 'UK', 'gb - doncaster': 'UK', 'gb - epsom downs': 'UK', 'gb - exeter': 'UK',
    'gb - fakenham': 'UK', 'gb - ffos las': 'UK', 'gb - fontwell': 'UK', 'gb - goodwood': 'UK',
    'gb - hamilton': 'UK', 'gb - haydock': 'UK', 'gb - hereford': 'UK', 'gb - hexham': 'UK',
    'gb - huntingdon': 'UK', 'gb - kelso': 'UK', 'gb - kempton': 'UK', 'gb - leicester': 'UK',
    'gb - lingfield': 'UK', 'gb - ludlow': 'UK', 'gb - market rasen': 'UK', 'gb - musselburgh': 'UK',
    'gb - newbury': 'UK', 'gb - newcastle': 'UK', 'gb - newmarket': 'UK', 'gb - newton abbot': 'UK',
    'gb - nottingham': 'UK', 'gb - perth': 'UK', 'gb - plumpton': 'UK', 'gb - pontefract': 'UK',
    'gb - redcar': 'UK', 'gb - ripon': 'UK', 'gb - salisbury': 'UK', 'gb - sandown': 'UK',
    'gb - sedgefield': 'UK', 'gb - southwell': 'UK', 'gb - stratford-on-avon': 'UK', 'gb - taunton': 'UK',
    'gb - thirsk': 'UK', 'gb - towcester': 'UK', 'gb - uttoxeter': 'UK', 'gb - warwick': 'UK',
    'gb - wetherby': 'UK', 'gb - wincanton': 'UK', 'gb - windsor': 'UK', 'gb - wolverhampton': 'UK',
    'gb - worcester': 'UK', 'gb - yarmouth': 'UK', 'gb - york': 'UK',
    'hong kong happy valley': 'HK', 'hong kong sha tin': 'HK',
    'in - bangalore': 'IN', 'in - chennai': 'IN', 'in - hyderabad': 'IN', 'in - kolkata': 'IN',
    'in - mumbai': 'IN', 'in - mysore': 'IN', 'in - ooty': 'IN', 'in - pune': 'IN',
    'ie - ballinrobe': 'IRE', 'ie - bellewstown': 'IRE', 'ie - clonmel': 'IRE', 'ie - cork': 'IRE',
    'ie - curragh': 'IRE', 'ie - down royal': 'IRE', 'ie - downpatrick': 'IRE', 'ie - dundalk': 'IRE',
    'ie - fairyhouse': 'IRE', 'ie - galway': 'IRE', 'ie - gowran': 'IRE', 'ie - kilbeggan': 'IRE',
    'ie - killarney': 'IRE', 'ie - laytown': 'IRE', 'ie - leopardstown': 'IRE', 'ie - limerick': 'IRE',
    'ie - listowel': 'IRE', 'ie - naas': 'IRE', 'ie - navan': 'IRE', 'ie - punchestown': 'IRE',
    'ie - roscommon': 'IRE', 'ie - sligo': 'IRE', 'ie - thurles': 'IRE', 'ie - tipperary': 'IRE',
    'ie - tramore': 'IRE', 'ie - wexford': 'IRE',
    'it - albenga harness': 'IT', 'it - chilivani': 'IT', 'it - florence': 'IT', 'it - foggia harness': 'IT',
    'it - follonica': 'IT', 'it - ivorno': 'IT', 'it - milan': 'IT', 'it - montecatini harness': 'IT',
    'it - padova harness': 'IT', 'it - sassari': 'IT', 'it - taranto harness': 'IT',
    'jm - caymanas': 'JM',
    'jp - funabashi': 'JP', 'jp - kawasaki': 'JP', 'jp - mombetsu': 'JP', 'jp - tokyo city keiba': 'JP', 'jp - urawa': 'JP',
    'korea busan friday': 'KR', 'korea busan thurs / sat': 'KR', 'korea seoul friday': 'KR', 'korea seoul thurs / sat': 'KR',
    'my - selangor turf club': 'MY',
    'no - bergen': 'NO', 'no - biri': 'NO', 'no - bjerke': 'NO', 'no - bodo': 'NO', 'no - forus': 'NO',
    'no - harstad': 'NO', 'no - jarlsberg': 'NO', 'no - klosterskogen': 'NO', 'no - momarken': 'NO',
    'no - orkla': 'NO', 'no - ovrevoll': 'NO', 'no - sorlandet': 'NO',
    'nz - arawa': 'NZ', 'nz - ascot': 'NZ', 'nz - ashburton': 'NZ', 'nz - avondale': 'NZ', 'nz - awapuni': 'NZ',
    'nz - cambridge': 'NZ', 'nz - cromwell': 'NZ', 'nz - dargaville': 'NZ', 'nz - ellerslie': 'NZ',
    'nz - gore': 'NZ', 'nz - hastings': 'NZ', 'nz - hawera': 'NZ', 'nz - hokitika': 'NZ', 'nz - kumara': 'NZ',
    'nz - kurow': 'NZ', 'nz - makaraka': 'NZ', 'nz - matamata': 'NZ', 'nz - motukarara': 'NZ',
    'nz - new plymouth': 'NZ', 'nz - oamaru': 'NZ', 'nz - omakau': 'NZ', 'nz - omoto': 'NZ', 'nz - orari': 'NZ',
    'nz - otaki': 'NZ', 'nz - phar lap': 'NZ', 'nz - pukekohe': 'NZ', 'nz - reefton': 'NZ',
    'nz - riccarton': 'NZ', 'nz - riverton': 'NZ', 'nz - ruakaka': 'NZ', 'nz - stratford': 'NZ',
    'nz - tauherenikau': 'NZ', 'nz - taupo': 'NZ', 'nz - tauranga': 'NZ', 'nz - te aroha': 'NZ',
    'nz - te awamutu': 'NZ', 'nz - te rapa': 'NZ', 'nz - te teko': 'NZ', 'nz - thames': 'NZ',
    'nz - trentham': 'NZ', 'nz - waikouaiti': 'NZ', 'nz - waimate': 'NZ', 'nz - waipukurau': 'NZ',
    'nz - wairoa': 'NZ', 'nz - wanganui': 'NZ', 'nz - waterlea': 'NZ', 'nz - waverley': 'NZ',
    'nz - wingatui': 'NZ', 'nz - winton': 'NZ', 'nz - woodville': 'NZ', 'nz - wyndham': 'NZ',
    'saudi cup': 'SA', 'singapore': 'SG',
    'se - aby': 'SE', 'se - amal': 'SE', 'se - arjang': 'SE', 'se - arvika': 'SE', 'se - axevalla': 'SE',
    'se - bergsaker': 'SE', 'se - boden': 'SE', 'se - bollnas': 'SE', 'se - bro': 'SE',
    'se - dannero': 'SE', 'se - eskilstuna': 'SE', 'se - farjestad': 'SE', 'se - gardets galopp': 'SE',
    'se - gavle': 'SE', 'se - goteborg galopp': 'SE', 'se - hagmyren': 'SE', 'se - halmstad': 'SE',
    'se - hoting': 'SE', 'se - jagersro': 'SE', 'se - jagersro galopp': 'SE', 'se - kalmar': 'SE',
    'se - lindesberg': 'SE', 'se - lycksele': 'SE', 'se - mantorp': 'SE', 'se - mariehamn': 'SE',
    'se - orebro': 'SE', 'se - ostersund': 'SE', 'se - oviken': 'SE', 'se - rattvik': 'SE',
    'se - romme': 'SE', 'se - skelleftea': 'SE', 'se - solanget': 'SE', 'se - solvalla': 'SE',
    'se - tingsryd': 'SE', 'se - umaker': 'SE', 'se - vaggeryd': 'SE', 'se - visby': 'SE',
    'las piedras': 'UY',
    'za - clairwood': 'ZA', 'za - durbanville': 'ZA', 'za - fairview': 'ZA', 'za - flamingo': 'ZA',
    'za - greyville': 'ZA', 'za - kenilworth': 'ZA', 'za - mauritius': 'ZA', 'za - scottsville': 'ZA',
    'za - turffontein': 'ZA', 'za - vaal': 'ZA',
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def normalize_track_name(name: str) -> str:
    if not isinstance(name, str): return ""
    return name.lower().strip().replace('(july)', '').replace('(aw)', '').replace('acton', '').replace('park', '').strip()

def fetch_page(url: str):
    print(f"-> Fetching: {url}")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=20)
        response.raise_for_status()
        print("   ✅ Success.")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Failed to fetch page: {e}")
        return None

def sort_and_limit_races(races: list[dict], limit: int = 20) -> list[dict]:
    print(f"\n⏳ Filtering for upcoming races, sorting, and limiting...")
    now_utc = datetime.now(pytz.utc)
    future_races = [race for race in races if race.get('datetime_utc') and race['datetime_utc'] > now_utc]
    future_races.sort(key=lambda r: r['datetime_utc'])
    limited_races = future_races[:limit]
    print(f"   -> Found {len(future_races)} upcoming races. Limiting to a maximum of {len(limited_races)}.")
    return limited_races

# ==============================================================================
# STEP 1: UNIVERSAL SCANS
# ==============================================================================

def universal_sky_sports_scan(html_content: str, base_url: str, today_date: date):
    if not html_content: return []
    print("\n🔍 Starting Universal Scan of Sky Sports...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races, source_tz = [], pytz.timezone('Europe/London')
    def get_country_code(title): return re.search(r'\((\w+)\)$', title).group(1).upper() if re.search(r'\((\w+)\)$', title) else "UK"
    def parse_url(url):
        try:
            parts = urlparse(url).path.strip('/').split('/'); idx = parts.index('racecards')
            return parts[idx+1].replace('-',' ').title(), parts[idx+2]
        except (ValueError, IndexError): return None, None
    for block in soup.find_all('div', class_='sdc-site-concertina-block'):
        title_tag = block.find('h3', class_='sdc-site-concertina-block__title')
        if not title_tag: continue
        country = get_country_code(title_tag.get_text(strip=True))
        events = block.find('div', class_='sdc-site-racing-meetings__events')
        if not events: continue
        for container in events.find_all('div', class_='sdc-site-racing-meetings__event'):
            link = container.find('a', class_='sdc-site-racing-meetings__event-link')
            if not link: continue
            url, (course, date_str) = urljoin(base_url, link.get('href')), parse_url(urljoin(base_url, link.get('href')))
            if not course or not date_str: continue
            try:
                if datetime.strptime(date_str, '%d-%m-%Y').date() != today_date: continue
            except ValueError: continue
            details = container.find('span', class_='sdc-site-racing-meetings__event-details')
            runners = re.search(r'(\d+)\s+runners?', details.get_text(strip=True), re.IGNORECASE) if details else None
            field = int(runners.group(1)) if runners else 0
            time_tag = container.find('span', class_='sdc-site-racing-meetings__event-time')
            race_time = time_tag.get_text(strip=True) if time_tag else "N/A"
            utc_time = None
            if race_time != "N/A":
                try: utc_time = source_tz.localize(datetime.strptime(f"{date_str} {race_time}", '%d-%m-%Y %H:%M')).astimezone(pytz.utc)
                except (ValueError, KeyError): pass
            all_races.append({'course': course, 'time': race_time, 'field_size': field, 'race_url': url,
                              'country': country, 'date_iso': date_str, 'datetime_utc': utc_time})
            print(f"   -> Found Today's Race: {course} ({country}) at {race_time} [Europe/London]")
    print(f"✅ Sky Sports Scan complete. Found {len(all_races)} races for today.")
    return all_races

def universal_sporting_life_scan(html_content: str, base_url: str, today_date: date):
    if not html_content: return []
    print("\n🔍 Starting Universal Scan of Sporting Life...")
    soup = BeautifulSoup(html_content, 'html.parser')
    all_races, processed = [], set()
    for link in soup.find_all('a', href=re.compile(r'/racing/racecards/....-..-../.*/racecard/')):
        try:
            parts = urlparse(link.get('href')).path.strip('/').split('/'); idx = parts.index('racecards')
            date_url, course = parts[idx+1], parts[idx+2].replace('-',' ').title()
            if datetime.strptime(date_url, '%Y-%m-%d').date() != today_date: continue
        except (ValueError, IndexError): continue
        parent = link.parent
        if not parent: continue
        race_time = None
        time_tag = parent.find_previous_sibling('span')
        if time_tag and (match := re.search(r'(\d{2}:\d{2})', time_tag.text)): race_time = match.group(1)
        if not race_time and (gparent := parent.parent) and (match := re.search(r'(\d{2}:\d{2})', gparent.text)): race_time = match.group(1)
        if not race_time: continue
        key = (normalize_track_name(course), race_time)
        if key in processed: continue
        processed.add(key)
        runners = re.search(r'(\d+)\s+Runners', link.get_text(strip=True), re.IGNORECASE)
        field = int(runners.group(1)) if runners else 0
        country = COURSE_TO_COUNTRY_MAP.get(normalize_track_name(course), 'UK')
        utc_time, date_iso = None, None
        try:
            tz = pytz.timezone(TIMEZONE_MAP.get(country, 'Europe/London'))
            naive = datetime.strptime(f"{date_url} {race_time}", '%Y-%m-%d %H:%M')
            utc_time = tz.localize(naive).astimezone(pytz.utc)
            date_iso = naive.strftime('%d-%m-%Y')
        except (ValueError, KeyError): continue
        all_races.append({'course': course, 'time': race_time, 'field_size': field, 'race_url': urljoin(base_url, link.get('href')),
                          'country': country, 'date_iso': date_iso, 'datetime_utc': utc_time})
        print(f"   -> Found Today's Race: {course} ({country}) at {race_time} [{tz.zone}]")
    print(f"✅ Sporting Life Scan complete. Found {len(all_races)} races for today.")
    return all_races

def parse_equibase_local_file(filepath: str, today_date: date):
    """Parses a local EquibaseToday.txt file to extract race data."""
    if not os.path.exists(filepath):
        print(f"\n- Local file not found: {filepath}. Skipping.")
        return []

    print(f"\n🔍 Starting Local Scan of {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    all_races = []
    track_sections = content.split("Equibase\nAccount")

    equibase_tz_map = {'ET': 'America/New_York', 'CT': 'America/Chicago', 'MT': 'America/Denver', 'PT': 'America/Los_Angeles'}

    for section in track_sections:
        if not section.strip(): continue

        course_match = re.search(r"(.+?)\s*\|\s*\w+\s+\d+,\s+\d{4}", section)
        if not course_match: continue
        course_name = course_match.group(1).strip()

        race_lines = re.findall(r"^\d+\s+.+?\s+(\d{1,2}:\d{2}\s*(?:AM|PM)\s*\wT)\s+(\d+)\s+Starters", section, re.MULTILINE)

        for race_time_full, starters in race_lines:
            field_size = int(starters)
            time_parts = race_time_full.split()
            race_time_str, tz_abbr = f"{time_parts[0]} {time_parts[1]}", time_parts[2]

            country = COURSE_TO_COUNTRY_MAP.get(normalize_track_name(course_name), 'USA')
            utc_time, date_iso = None, None

            try:
                if (tz_name := equibase_tz_map.get(tz_abbr)):
                    source_tz = pytz.timezone(tz_name)
                    naive_dt = datetime.strptime(f"{today_date.strftime('%Y-%m-%d')} {race_time_str}", '%Y-%m-%d %I:%M %p')
                    utc_time = source_tz.localize(naive_dt).astimezone(pytz.utc)
                    date_iso = today_date.strftime('%d-%m-%Y')

                all_races.append({
                    'course': course_name, 'time': naive_dt.strftime('%H:%M'), 'field_size': field_size,
                    'race_url': '', 'country': country, 'date_iso': date_iso, 'datetime_utc': utc_time
                })
                print(f"   -> Found Local Race: {course_name} ({country}) at {race_time_str} [{tz_name}]")
            except (ValueError, KeyError, pytz.UnknownTimeZoneError) as e:
                print(f"   -> Could not process race for {course_name} at {race_time_full}. Error: {e}")

    print(f"✅ Local Equibase Scan complete. Found {len(all_races)} races for today.")
    return all_races

# ==============================================================================
# STEP 2 & MODE A (UNCHANGED)
# ==============================================================================

def check_attheraces_connectivity(url="https://www.attheraces.com/"):
    print("\n🌐 Performing Environmental Check..."); print(f"-> Pinging: {url}")
    try:
        r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, verify=False, timeout=10, stream=True); r.raise_for_status()
        print(f"   ✅ Success! Network is UNRESTRICTED (Status: {r.status_code})."); return True
    except requests.exceptions.RequestException as e:
        print(f"   ❌ AtTheRaces is unreachable. Network is RESTRICTED. Reason: {e}"); return False

def convert_odds_to_float(odds_str: str) -> float:
    if not isinstance(odds_str, str): return 9999.0
    s = odds_str.strip().upper()
    if 'SP' in s: return 9999.0
    if s == 'EVS': return 1.0
    if '/' in s:
        try: n, d = map(float, s.split('/')); return n/d if d != 0 else 9999.0
        except (ValueError, IndexError): return 9999.0
    try: return float(s)
    except ValueError: return 9999.0

def fetch_atr_odds_data(regions: list[str]) -> dict:
    print("\n📡 Fetching Live Odds from AtTheRaces...")
    today_str = datetime.now().strftime('%Y%m%d'); lookup = {}
    for region in regions:
        url = f"https://www.attheraces.com/ajax/marketmovers/tabs/{region}/{today_str}"; print(f"-> Querying {region.upper()} from: {url}")
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=15); r.raise_for_status()
            if not r.text: continue
        except requests.exceptions.RequestException as e: print(f"   ❌ ERROR: {e}"); continue
        soup = BeautifulSoup(r.text, 'html.parser')
        for caption in soup.find_all('caption', string=re.compile(r"^\d{2}:\d{2}")):
            time_match = re.match(r"(\d{2}:\d{2})", caption.get_text(strip=True))
            if not time_match: continue
            race_time = time_match.group(1)
            course_header = caption.find_parent('div', class_='panel').find('h2')
            if not course_header: continue
            course_name = course_header.get_text(strip=True)
            table = caption.find_next_sibling('table')
            if not table: continue
            horses = [{'name': c[0].get_text(strip=True), 'odds_str': c[1].get_text(strip=True)} for row in table.find('tbody').find_all('tr') if (c := row.find_all('td'))]
            if not horses: continue
            for h in horses: h['odds_float'] = convert_odds_to_float(h['odds_str'])
            horses.sort(key=lambda x: x['odds_float'])
            lookup[(normalize_track_name(course_name), race_time)] = {'course': course_name, 'time': race_time, 'field_size': len(horses),
                'favorite': horses[0] if horses else None, 'second_favorite': horses[1] if len(horses) > 1 else None}
    print(f"✅ AtTheRaces scan complete. Found data for {len(lookup)} races.")
    return lookup

def generate_mode_A_report(races: list[dict]):
    title = "Perfect Tipsheet"; filename = f"Perfect_Tipsheet_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.html"
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f4f8; color: #333; margin: 20px; }
        .container { max-width: 800px; margin: auto; background: #fff; padding: 25px; border-radius: 10px; box-shadow: 0 5px 15px rgba(0,0,0,0.1); }
        h1 { color: #1a73e8; text-align: center; border-bottom: 3px solid #1a73e8; padding-bottom: 10px; }
        .race-card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-left: 5px solid #1a73e8; background-color: #fff; border-radius: 8px; }
        .race-header { font-size: 1.5em; font-weight: bold; color: #333; margin-bottom: 15px; } .race-meta { font-size: 1.1em; color: #5f6368; margin-bottom: 15px; }
        .horse-details { margin-top: 10px; padding: 10px; border-radius: 5px; background-color: #f8f9fa; } .horse-details b { color: #1a73e8; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #777; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""
    if not races: html_body += "<p>No upcoming races in the next 30 minutes met the specified criteria of Field Size between 3 and 6, Favorite >= 1/1, and 2nd Favorite >= 3/1.</p>"
    else:
        html_body += f"<p>Found {len(races)} races that meet all analytical criteria.</p>"
        for race in races:
            fav, sec_fav = race['favorite'], race['second_favorite']
            html_body += f"""<div class="race-card">
                <div class="race-header">{race['course']} - Race Time: {race['time']}</div>
                <div class="race-meta">Field Size: {race['field_size']} Runners</div>
                <div class="horse-details"><b>Favorite:</b> {fav['name']} (<b>{fav['odds_str']}</b>)</div>
                <div class="horse-details"><b>2nd Favorite:</b> {sec_fav['name']} (<b>{sec_fav['odds_str']}</b>)</div></div>"""
    html_end = f'<div class="footer"><p>Report generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p></div></div></body></html>'
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(html_start + html_body + html_end)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e: print(f"\n❌ Error saving the report: {e}")

def run_mode_A(master_race_list: list[dict]):
    print("\n-- Running Mode A: Unrestricted Workflow --")
    small_field_races = [r for r in master_race_list if 3 <= r.get('field_size', 0) <= 6]
    print(f"Found {len(small_field_races)} races with 3 to 6 runners.")
    if not small_field_races: generate_mode_A_report([]); return
    atr_regions = ['uk', 'ireland', 'usa', 'france', 'saf', 'aus']
    atr_odds_data = fetch_atr_odds_data(atr_regions)
    if not atr_odds_data: print("Could not fetch any live odds from AtTheRaces."); return
    print("\n🔍 Analyzing races against final criteria (next 30 mins, Fav >= 1/1, 2nd Fav >= 3/1)...")
    now_utc, end_time = datetime.now(pytz.utc), datetime.now(pytz.utc) + timedelta(minutes=30)
    perfect_tips = []
    for race in small_field_races:
        if not (race['datetime_utc'] and now_utc < race['datetime_utc'] < end_time): continue
        key = (normalize_track_name(race['course']), race['time'])
        if key in atr_odds_data:
            atr_data = atr_odds_data[key]
            fav, sec_fav = atr_data.get('favorite'), atr_data.get('second_favorite')
            if not (fav and sec_fav): continue
            if fav['odds_float'] >= 1.0 and sec_fav['odds_float'] >= 3.0:
                race['favorite'], race['second_favorite'] = fav, sec_fav
                perfect_tips.append(race)
                print(f"   ✅ MATCH: {race['course']} {race['time']}")
    perfect_tips.sort(key=lambda r: r['datetime_utc'])
    generate_mode_A_report(perfect_tips)

# ==============================================================================
# MODE B: RESTRICTED WORKFLOW (WITH NEW LINK GENERATION)
# ==============================================================================

class RacingAndSportsFetcher:
    def __init__(self, api_url):
        self.api_url = api_url; self.session = requests.Session()
        headers = {'User-Agent':'Mozilla/5.0', 'Accept':'application/json, text/plain, */*', 'Referer':'https://www.racingandsports.com.au/todays-racing'}
        self.session.headers.update(headers)
    def fetch_data(self):
        print("-> Fetching R&S main meeting directory...")
        try: r = self.session.get(self.api_url, timeout=30, verify=False); r.raise_for_status(); return r.json()
        except requests.exceptions.RequestException as e: print(f"   ❌ ERROR: Could not fetch R&S JSON: {e}")
        except json.JSONDecodeError: print("   ❌ ERROR: Failed to decode R&S JSON.")
        return None
    def process_meetings_data(self, json_data):
        if not isinstance(json_data, list): return None
        meetings = []
        for discipline in json_data:
            for country in discipline.get("Countries", []):
                for meeting in country.get("Meetings", []):
                    if (course := meeting.get("Course")) and (link := meeting.get("PDFUrl") or meeting.get("PreMeetingUrl")):
                        meetings.append({'course': course, 'link': link})
        return meetings

def build_rs_lookup_table(rs_meetings):
    lookup = {}
    if not rs_meetings: return lookup
    print("...Building Racing & Sports lookup table for matching...")
    for meeting in rs_meetings:
        link = meeting.get('link')
        match = re.search(r'/(\d{4}-\d{2}-\d{2})', link)
        if not match: continue
        lookup[(normalize_track_name(meeting['course']), match.group(1))] = link
    print(f"   ✅ Lookup table created with {len(lookup)} R&S entries.")
    return lookup

def find_rs_link(track: str, date_iso: str, lookup: dict):
    try: date_yyyymmdd = datetime.strptime(date_iso, '%d-%m-%Y').strftime('%Y-%m-%d')
    except (ValueError, TypeError): return None
    norm_track = normalize_track_name(track)
    if (direct_key := (norm_track, date_yyyymmdd)) in lookup: return lookup[direct_key]
    for (rs_track, rs_date), rs_link in lookup.items():
        if rs_date == date_yyyymmdd and (norm_track in rs_track or rs_track in norm_track): return rs_link
    return None

def generate_external_links(race: dict) -> dict:
    """Generates the Brisnet and AtTheRaces links for a given race."""
    course, date_iso = race.get('course'), race.get('date_iso')
    if not course or not date_iso: return race
    try:
        date_obj = datetime.strptime(date_iso, '%d-%m-%Y')
        # Brisnet: Churchill-Downs / 2023-11-22
        brisnet_course = course.replace(' ', '-')
        brisnet_date = date_obj.strftime('%Y-%m-%d')
        race['brisnet_url'] = f"https://www.brisnet.com/racings-entries-results/USA/{brisnet_course}/{brisnet_date}"
        # AtTheRaces: saratoga / 2023-07-20
        atr_course = course.replace(' ', '-').lower()
        atr_date = date_obj.strftime('%Y-%m-%d')
        race['atr_url'] = f"https://www.attheraces.com/racecards/{atr_course}/{atr_date}"
    except (ValueError, TypeError): pass
    return race

def generate_mode_B_report(races: list[dict]):
    title = "Upcoming Small-Field Races"; filename = f"Actionable_Link_List_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.html"
    html_css = """<style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f4f4f9; color: #333; margin: 20px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        h1 { color: #5a2d82; text-align: center; border-bottom: 3px solid #5a2d82; padding-bottom: 10px; }
        .course-group { margin-bottom: 30px; } .course-header { font-size: 1.6em; font-weight: bold; color: #333; padding-bottom: 10px; border-bottom: 2px solid #eee; margin-bottom: 15px; }
        .race-entry { border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; background-color: #fafafa; }
        .race-details { font-weight: bold; font-size: 1.1em; color: #333; margin-bottom: 10px; }
        .race-links a, .race-links span { display: inline-block; text-decoration: none; padding: 8px 15px; border-radius: 4px; margin: 5px 5px 5px 0; font-weight: bold; min-width: 130px; text-align: center; }
        a.sky-link { background-color: #007bff; color: white; } a.sky-link:hover { background-color: #0056b3; }
        a.rs-link { background-color: #dc3545; color: white; } a.rs-link:hover { background-color: #c82333; }
        a.atr-link { background-color: #ffc107; color: black; } a.atr-link:hover { background-color: #e0a800; }
        a.brisnet-link { background-color: #28a745; color: white; } a.brisnet-link:hover { background-color: #218838; }
        span.rs-ignored-tag { color: #6c757d; background-color: #fff; border: 1px solid #ccc; cursor: default; }
    </style>"""
    html_start = f'<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{title}</title>{html_css}</head><body><div class="container"><h1>{title}</h1>'
    html_body = ""
    if not races: html_body += "<p>No upcoming races with 3 to 6 runners were found on any of the tracked sources today.</p>"
    else:
        races_by_course, course_order, seen = {}, [], set()
        for race in races:
            races_by_course.setdefault(race['course'], []).append(race)
            if race['course'] not in seen: course_order.append(race['course']); seen.add(race['course'])
        for course in course_order:
            html_body += f'<div class="course-group"><div class="course-header">{course}</div>'
            for race in races_by_course[course]:
                html_body += f'<div class="race-entry"><p class="race-details">Race at {race["time"]} ({race["field_size"]} runners)</p><div class="race-links">'
                html_body += f'<a href="{race["race_url"]}" target="_blank" class="sky-link">Sky Sports</a>'
                if race.get("rs_link"): html_body += f'<a href="{race["rs_link"]}" target="_blank" class="rs-link">R&S Form</a>'
                elif race['country'] in ['UK', 'IRE']: html_body += '<span class="rs-ignored-tag">Ignored by R&S</span>'
                if race.get("atr_url"): html_body += f'<a href="{race["atr_url"]}" target="_blank" class="atr-link">AtTheRaces</a>'
                if race.get("brisnet_url"): html_body += f'<a href="{race["brisnet_url"]}" target="_blank" class="brisnet-link">Brisnet</a>'
                html_body += '</div></div>'
            html_body += '</div>'
    html_end = f'<div class="footer"><p>Report generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p></div></div></body></html>'
    final_html = html_start + html_body + html_end
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(final_html)
        print(f"\n🎉 SUCCESS! Report generated: {os.path.abspath(filename)}")
    except Exception as e: print(f"\n❌ Error saving the report: {e}")

def run_mode_B(master_race_list: list[dict]):
    print("\n-- Running Mode B: Restricted Workflow --")
    small_field_races = [r for r in master_race_list if 3 <= r.get('field_size', 0) <= 6]
    print(f"Found {len(small_field_races)} races with 3 to 6 runners.")
    if not small_field_races: generate_mode_B_report([]); return
    print("\n🗞️ Fetching data from Racing & Sports...")
    rs_api_url = "https://www.racingandsports.com.au/todays-racing-json-v2"
    link_fetcher = RacingAndSportsFetcher(rs_api_url)
    json_data = link_fetcher.fetch_data()
    all_rs_meetings = link_fetcher.process_meetings_data(json_data) if json_data else []
    rs_lookup_table = build_rs_lookup_table(all_rs_meetings)
    print("\n🔗 Enriching races with external links...")
    enriched_races = []
    for race in small_field_races:
        # Add R&S links
        if race['country'] in ['UK', 'IRE', 'FR', 'SAF', 'USA', 'AUS', 'URU']:
            if date_iso := race.get('date_iso'):
                if rs_link := find_rs_link(race['course'], date_iso, rs_lookup_table):
                    race['rs_link'] = rs_link
                    print(f"   -> {race['course']} @ {race['time']}: R&S Link FOUND")
        # Add Brisnet and ATR links for all races
        race = generate_external_links(race)
        enriched_races.append(race)
    enriched_races = sort_and_limit_races(enriched_races)
    generate_mode_B_report(enriched_races)

# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================

def fetch_rpb2b_api_data(today_date: date):
    """Fetches race data from the RPB2B JSON API for North America."""
    print("\n🔍 Starting API Scan of RPB2B API for North American races...")
    api_date = today_date.strftime('%Y-%m-%d')
    url = f"https://backend-us-racecards.widget.rpb2b.com/v2/racecards/daily/{api_date}"
    print(f"-> Querying API: {url}")

    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        response.raise_for_status()
        data = response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"   ❌ ERROR: Failed to fetch or decode RPB2B API data: {e}")
        return None

    all_races = []
    for meeting in data:
        course_name, country_code = meeting.get('name'), meeting.get('countryCode')
        if not course_name or not country_code: continue

        for race in meeting.get('races', []):
            utc_time_str = race.get('datetimeUtc')
            if not utc_time_str: continue

            utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
            local_tz = pytz.timezone(TIMEZONE_MAP.get(country_code, 'America/New_York'))
            local_time = utc_time.astimezone(local_tz)

            all_races.append({
                'course': course_name, 'time': local_time.strftime('%H:%M'),
                'field_size': race.get('numberOfRunners', 0), 'race_url': '',
                'country': country_code, 'date_iso': local_time.strftime('%d-%m-%Y'),
                'datetime_utc': utc_time
            })
    print(f"✅ RPB2B API Scan complete. Found {len(all_races)} North American races.")
    return all_races

def main():
    print("=" * 80); print("🚀 Unified Racing Report Generator"); print("=" * 80)
    user_tz = pytz.timezone("America/New_York")
    today = datetime.now(user_tz).date()
    print(f"📅 Operating on User's Date: {today.strftime('%Y-%m-%d')}")
    races_dict = {}

    # --- Step 1: Fetch North American data from the primary API ---
    na_races = fetch_rpb2b_api_data(today)
    if na_races is None: # API failed, try local file as fallback
        print("   -> RPB2B API failed. Falling back to local file for North America...")
        na_races = parse_equibase_local_file('EquibaseToday.txt', today)

    for race in na_races:
        key = (normalize_track_name(race['course']), race['time'])
        if key not in races_dict: races_dict[key] = race

    # --- Step 2: Scrape Web Sources for the rest of the world ---
    web_sources = [
        {"name": "Sky Sports", "url": "https://www.skysports.com/racing/racecards", "scraper": universal_sky_sports_scan},
        {"name": "Sporting Life", "url": "https://www.sportinglife.com/racing/racecards", "scraper": universal_sporting_life_scan},
    ]
    for source in web_sources:
        print(f"\n--- Processing Web Source: {source['name']} ---")
        html_content = fetch_page(source['url'])
        if html_content:
            races = source['scraper'](html_content, source['url'], today)
            print(f"\nProcessing and merging {len(races)} races from {source['name']}...")
            for race in races:
                # Only add if it's not a North American race to avoid conflicts
                if race.get('country') not in ['USA', 'CAN']:
                    key = (normalize_track_name(race['course']), race['time'])
                    if key not in races_dict:
                        races_dict[key] = race
                        print(f"   -> Added new race: {race['course']} {race['time']}")
                    elif (new_size := race.get('field_size', 0)) > races_dict[key].get('field_size', 0):
                        print(f"   -> Updating field size for {race['course']} {race['time']} to {new_size}")
                        races_dict[key]['field_size'] = new_size

    master_race_list = list(races_dict.values())
    print(f"\nTotal unique races found for today: {len(master_race_list)}")
    if not master_race_list:
        print("\nCould not retrieve any race list for today. Exiting."); return

    # --- Step 3: Run Appropriate Mode ---
    if check_attheraces_connectivity():
        run_mode_A(master_race_list)
    else:
        run_mode_B(master_race_list)

if __name__ == "__main__":
    main()
