import os
import sys
import math
import csv
import shutil
import requests
import re
from datetime import datetime
from difflib import SequenceMatcher
from feedgen.feed import FeedGenerator
from git import Repo
import time

minPrice = 1720
maxPrice = 2500
minBeds = 2
maxBeds = None
bHasPic = True

# Only focus on the last __ hrs
timeRangeHrs = 24*3

# File to which data is stored
csvFilename = "listings.txt"

# folder to which we'll save things
saveFolder = "feeds"

# Url to which rss feeds are exported
githubRepoURL = "https://raw.githubusercontent.com/bensnell/apts-nyc/master/"
repoName = "apts-nyc"

# Refresh Minutes
refreshMin = 15

# Tool: https://codepen.io/jhawes/pen/ujdgK
bOneHoodPerListing = True
bounds = {}
bounds[((40.691111, -73.997705), (40.679219, -74.003612), (40.674112, -73.996844), (40.679467, -73.988390), (40.686652, -73.983583))] = "CarollGardens_CobbleHill_F"
bounds[((40.674112, -73.996844), (40.679467, -73.988390), (40.681648, -73.975694), (40.672457, -73.969629), (40.659928, -73.980828), (40.667024, -73.995189))] = "ParkSlope_Gowanus_DR"
bounds[((40.667332, -73.996660), (40.656607, -74.012609), (40.647140, -74.011767), (40.649853, -74.001741), (40.662371, -73.988654))] = "Greenwood_DR"
bounds[((40.664352, -73.964613), (40.663630, -73.955147), (40.652422, -73.954009), (40.652995, -73.966709))] = "ProspectLeffertsGardens_Q"
bounds[((40.666418, -73.962165), (40.665446, -73.950871), (40.669550, -73.945130), (40.678323, -73.946887), (40.683929, -73.977162), (40.672776, -73.969809))] = "ProspectHeights_CrownHeightsNorth_245_A"
bounds[((40.678323, -73.946887), (40.683929, -73.977162), (40.689866, -73.980422), (40.689510, -73.972085), (40.683267, -73.950200), (40.680979, -73.916911), (40.676524, -73.917107))] = "ClintonHill_BedStudy_AC"
bounds[((40.692564, -73.934033), (40.695982, -73.922097), (40.696990, -73.907325), (40.702205, -73.908643), (40.701456, -73.934403), (40.708992, -73.950296), (40.712757, -73.959701), (40.706696, -73.963308), (40.701887, -73.950095))] = "Bushwick_Williamsburg_M"
bounds[((40.769745, -73.946294), (40.777085, -73.963851), (40.787943, -73.955907), (40.780391, -73.937319))] = "Yorkville_UpperUpperEast_NQ_456"
bounds[((40.787943, -73.955907), (40.780391, -73.937319), (40.789252, -73.937042), (40.795049, -73.950689))] = "LowerEastHarlem_6"
bounds[((40.753439, -73.913582), (40.752022, -73.930194), (40.756178, -73.936548), (40.772891, -73.921646), (40.769315, -73.913367), (40.762741, -73.915868), (40.759388, -73.908732))] = "Astoria_MR_NW"
bounds[((40.753439, -73.913582), (40.757988, -73.909784), (40.748521, -73.887961), (40.743805, -73.891471))] = "Woodside_MR"
bounds[((40.757962, -74.007400), (40.749790, -73.987766), (40.737340, -73.996881), (40.744125, -74.013123))] = "Chelsea_AE_12"
bounds[((40.769745, -73.946294), (40.777085, -73.963851), (40.764321, -73.973008), (40.757599, -73.957019))] = "LowerUpperEast_6_NQ"
bounds[((40.806426, -73.972133), (40.800501, -73.958118), (40.768016, -73.981694), (40.774089, -73.996288))] = "UpperWest_12_BC"
bounds[((40.720470, -73.994254), (40.714504, -73.974829), (40.707670, -73.979015), (40.709433, -74.001661))] = "LowerEast_F_BD"
bounds[((40.689678, -73.992394), (40.684185, -73.977565), (40.707027, -73.981427), (40.705107, -73.997907), (40.693915, -74.007863))] = "BrooklynHeights_R_245_AG_F"


# =============================================================


def getWorkingDir():
	return os.path.dirname(os.path.abspath(__file__))

csvPath = getWorkingDir() + "/" + csvFilename
saveFolderPath = getWorkingDir() + "/" + saveFolder
if not os.path.exists(saveFolderPath):
	os.makedirs(saveFolderPath)

def datetime2RSSString(dt):
	return dt.strftime('%Y-%m-%d %H:%M:%S') + "-05:00"

def inside_polygon(x, y, points):
    """
    Return True if a coordinate (x, y) is inside a polygon defined by
    a list of verticies [(x1, y1), (x2, x2), ... , (xN, yN)].

    Reference: http://www.ariel.com.au/a/python-point-int-poly.html
    """
    n = len(points)
    inside = False
    p1x, p1y = points[0]
    for i in range(1, n + 1):
        p2x, p2y = points[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

def clDate(stringDate):
	return datetime.strptime(stringDate, '%Y-%m-%d %H:%M')

def elapsedTimeHrs(was):

	delta = datetime.now() - was
	hrs = delta.total_seconds() / 3600
	return abs(hrs)

def similarity(a, b):
	return SequenceMatcher(None, str(a), str(b)).ratio()

# Load a csv (tab-separated) as an array
def loadCsv(path):
	listOut = []
	if (os.path.exists(path)):
		with open(path, 'r') as fin:
			data = fin.readlines()
			listOut = [[i.strip("\n") for i in x.split("\t")] for x in data]
	return listOut

# Save (append if exists) a tab csv from a 2d array
def saveCsv(array2d, path):
	with open(path, 'a+') as fout:
		for line in array2d:
			line = [str(i) for i in line]
			fout.write("\t".join(line) + "\n")

# Get 120 listings from 0 to 3000
def urlCL(startIndex):

	url = "https://newyork.craigslist.org/search/aap?"
	url += "&s=" + str(startIndex)
	if bHasPic: 
		url += "&hasPic=1"
	url += "&bundleDuplicates=1"
	if minPrice != None:
		url += "&min_price=" + str(minPrice)
	if maxPrice != None:
		url += "&max_price=" + str(maxPrice)
	if minBeds != None:
		url += "&min_bedrooms=" + str(minBeds)
	if maxBeds != None:
		url += "&max_bedrooms=" + str(maxBeds)
	url += "&availabilityMode=0"
	url += "&sale_date=all+dates"
	url += "&sort=date"
	return url

# Remove elements from the inList that are also in the refList by a given threshold
def removeMatches(inList, refList, indexKeys, threshold):

	newList = []
	if indexKeys == -1:

		for a in inList:
			bAdd = True
			for b in refList:
				if similarity(a, b) > threshold:
					bAdd = False
					break
			if bAdd:
				newList.append(a)

	else:

		for a in inList:

			bAdd = True
			for b in refList:

				bAllSimilar = True
				for index in indexKeys:
					bSimilar = similarity(a[index], b[index]) > threshold
					bAllSimilar = bAllSimilar and bSimilar
					if not bAllSimilar:
						break
				if bAllSimilar:
					bAdd = False

			if bAdd:
				newList.append(a)

	return newList


# Remove duplicates from a list or list of lists (provided indices)
# below a threshold of similarity
def removeDuplicates(thisList, indexKeys, threshold):

	newList = []
	if indexKeys == -1:

		# Match the elements of the list itself
		skipIndices = []
		for i in range(len(thisList)):
			bAdd = True
			for j in range(len(thisList)):
				if i == j: continue
				if i in skipIndices: 
					bAdd = False
					break
				if similarity(thisList[i], thisList[j]) > threshold:
					skipIndices.append(j)
			if bAdd:
				newList.append(thisList[i])

	else:
		skipIndices = []
		for i in range(len(thisList)):

			# Flag whether we're adding this one
			bAdd = True

			# Iterate through all other items
			for j in range(len(thisList)):

				# Don't compare the same element
				if i == j: continue

				# Skip elements that are repeated
				if i in skipIndices: 
					bAdd = False
					break

				bAllSimilar = True
				for index in indexKeys:
					bSimilar = similarity(thisList[i][index], thisList[j][index]) > threshold
					bAllSimilar = bAllSimilar and bSimilar
					if not bAllSimilar:
						break
				if bAllSimilar:
					skipIndices.append(j)

			if bAdd:
				newList.append(thisList[i])
	
	return newList

# Save a feed as an xml file
def saveFeed(listings, title, path):

	url = githubRepoURL + title + ".xml"

	# Create a feed generator
	fg = FeedGenerator()

	# Create the feed's title
	fg.id(url)
	fg.title(title)
	fg.author({'name':'Ben Snell'})
	fg.description("NYC 2BR Apartment Listings in " + title)
	fg.link( href=url, rel='alternate' )
	fg.language('en')
	time = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "-05:00"
	fg.pubDate(time)
	fg.updated(time)

	for apt in listings:

		e = fg.add_entry()
		
		e.id( apt[0] )
		e.title( "$" + apt[1] + "  //  " + apt[4] )
		e.link( href=apt[0] )

		text = ""
		if apt[5] != "":
			imgs = apt[5].split(" ")
			for i in range(len(imgs)):
				text += "<img src=\"" + imgs[i] + "\" /> "
				if i == 0:
					text += "<p>" + apt[8] + "</p>"
		else:
			text += "<p>" + apt[8] + "</p>"
		e.content( type="html", content=text )

		# This doesn't seem to work:
		e.pubDate( datetime2RSSString(clDate(apt[2])) )
		e.updated( datetime2RSSString(clDate(apt[2])) )

	fg.atom_str(pretty=True)
	fg.atom_file(path)

def scrapeCL():

	allApts = []

	# Iterate through all possible apartments
	for i in range(0, 3000, 120):

		print("Getting CL Apartments beginning with " + str(i))
		
		# Get the html text
		text = requests.get(urlCL(i), stream=False).text

		# parse into a list
		obj = re.findall(r'\<a href=\"(https://newyork.craigslist.org/.*?)\".*?\<span class=\"result-price\"\>\$(.*?)\</span\>.*?datetime=\"(.*?)\".*?data-id=\"(.*?)\" class=\"result-title hdrlnk\"\>(.*?)\</a\>', text, re.I | re.M | re.S)

		# add to the existing lists
		allApts.extend(obj)

	# Remove duplicates
	allApts = list(set(allApts))

	print("Removed Duplicates")

	# Convert into a list of lists (instead of tuples)
	allApts = [list(i) for i in allApts]

	# For each apartment, retrieve the listing and get information for an rss feed
	for i in range(len(allApts)):

		print("Retrieved listing for " + str(i) + " / " + str(len(allApts)) + " apts")

		# get the webpage
		text = requests.get(allApts[i][0], stream=False).text

		# Get all the images' urls and separate them by spaces
		obj = [i for i in re.findall(r'\"(https://images.craigslist.org/.*?)\"', text, re.I | re.M | re.S) if "600x450" in i]
		obj = list(set(obj))
		allApts[i].append(" ".join(obj))

		# Get the location
		objLat = re.findall(r'data-latitude=\"(.*?)\"', text, re.I | re.M | re.S)
		objLon = re.findall(r'data-longitude=\"(.*?)\"', text, re.I | re.M | re.S)
		if len(objLat) == 0 or len(objLon) == 0:
			allApts[i].append(0)
			allApts[i].append(0)
		else:
			allApts[i].append(float(objLat[0]))
			allApts[i].append(float(objLon[0]))

		# Get the full description
		# r'data-location=\"'+allApts[i][0]+
		obj = re.findall(r'\<section id=\"postingbody\"\>.*?\</div\>\n        \</div\>(.*?)\</section\>', text, re.I | re.M | re.S)
		if len(obj) == 0:
			allApts[i].append("")
		else:
			# obj[0] = obj[0].replace("<br>", "") # remove breaks
			obj[0] = obj[0].replace("\t", "") # remove tabs
			obj[0] = obj[0].replace("\n", "") # remove tabs
			allApts[i].append(obj[0])

	return allApts

def process():

	print("Starting a new process...")

	# Get all craigslist apartments
	allApts = scrapeCL();

	print("Scraped all CL Apartments")

	# Only take the ones that have been posted in the last 72 hrs
	allApts = [i for i in allApts if elapsedTimeHrs(clDate(i[2])) <= timeRangeHrs]

	print("Removed apartments < 3 days")

	# Only take the ones that have a location within the neighborhoods set already
	selectApts = []
	for apt in allApts:

		# Check whether this one is in any of the neighborhoods
		for key, value in bounds.items():
			if inside_polygon(apt[6], apt[7], list(key)):

				# Add the hood
				apt.append(value)

				# Save this into select
				selectApts.append(apt)

				# print(apt[4], apt[-1], apt[0])

				# one to one mapping
				if bOneHoodPerListing:
					break

	print("Narrowed down to specific hoods")

	# Sort by date to get the newest first
	selectApts.sort(key=lambda d: clDate(d[2]))
	selectApts.reverse()

	print("Sorted by date ", len(selectApts))

	# Load those apartments that have already been saved
	oldApts = loadCsv(csvPath)

	print("Loaded old apartments: ", len(oldApts))

	# Compare the new ones to the old ones for any matches and remove repeats
	selectApts = removeMatches(selectApts, oldApts, [4, 8], 0.95)

	print("Removed matches ", len(selectApts))

	# Remove duplicate listings (with the same title and body)
	selectApts = removeDuplicates(selectApts, [4, 8], 0.95)

	print("Removed self duplicates ", len(selectApts))

	# Save the old and new ones to file
	saveCsv(selectApts, csvPath)

	print("Saved new apts to file")

	# Get all neighborhoods
	feeds = {}
	for key, value in bounds.items():
		feeds[ value ] = []
	# Separate listings into represented hoods
	for apt in selectApts:
		if apt[9] in feeds:
			feeds[ apt[9] ].append( apt )
		else:
			feeds[ apt[9] ] = [ apt ]

	# Export new ones to an RSS feed file
	uploads = []
	for key, value in feeds.items():

		filename = key + ".xml"
		uploadName = saveFolderPath + "/" + filename
		saveFeed(value, key, uploadName)

		uploads.append(saveFolder + "/" + filename)
	uploads.append(csvFilename)

	print("Exported all to RSS Feeds")

	# Upload these files to github
	repo = Repo("../" + repoName)
	repo.index.add(uploads)
	repo.index.commit("Updated feeds")
	origin = repo.remote('origin')
	origin.push()

	print("Exported all to github")

while True:

	print("Starting Process ------------------------")

	# Run code
	start = time.time()
	process()
	stop = time.time()

	print("Ending Process ------------------------Waiting...")

	# Get duration in seconds
	duration = stop - start

	# Wait for no more than 15 minutes
	time.sleep(max(refreshMin*60 - duration, 0))

	print("... Done waiting")
