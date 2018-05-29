# -*- coding: utf-8 -*-
'''
@author smaddineni
@date Oct 17, 2017
'''
import os
import codecs
import _pickle as cPickle
import html.parser
from lxml import etree
import multiprocessing
import nltk
import os.path
import re
import sys
import zipfile
import csv
from nltk.corpus import stopwords
from textstat.textstat import textstat
import math, string, sys, fileinput

output_rows = []

# From Ero Carrera
# http://blog.dkbza.org/2007/05/scanning-data-for-entropy-anomalies.html
def range_bytes (): return range(256)

def range_printable(): return (ord(c) for c in string.printable)

def calculateEntropy(data, iterator=range_bytes):
    if not data:
        return 0
    entropy = 0
    for x in iterator():
        p_x = float(data.count(chr(x)))/len(data)
        if p_x > 0:
            entropy += - p_x*math.log(p_x, 2)
    return entropy

def getWordCount(sectiontext):
    totalwords = sectiontext.split()
    wordcount = len(totalwords)     
    return wordcount

def getAverageWordCount(sectiontext):
    totalwords = sectiontext.split()    
    if len(totalwords) is not 0:
        averagecount = sum(len(word) for word in totalwords) / len(totalwords) 
    return averagecount

def getFleschScore(sectiontext):
    fleschscore = textstat.flesch_reading_ease(sectiontext)
    return fleschscore

def getFleschKincaid(sectiontext):
    fleschkincaid = textstat.flesch_kincaid_grade(sectiontext)
    return fleschkincaid

def getBracketOccurrence(sectiontext):
    bracketOccurrences = sectiontext.count('(') + sectiontext.count('[') + sectiontext.count('{') + sectiontext.count(')') + sectiontext.count(']') + sectiontext.count('}') 
    return bracketOccurrences

def getConditionals(sectiontext):
    conditphrases = ['if','except','but','provided','when', 'where', 'unless', 'whenever', 'notwithstanding', 'in no event', 'in the event', 'in case', 'on the condition', 'wherever']        
    conditionals = len(re.findall(r"(?=("+'|'.join(conditphrases)+r"))", sectiontext))   
    return conditionals
    
def getVertexDepth(paraList):
    vertexDepth = 0
    level1 = 0   #lowercase letter 
    level2 = 0   #numbers
    level3 = 0  #roman numeral
    level4 = 0  #uppercase letter 
    alltext=''    
    vals = []
    try:
        for para in paraList: 
            etree.strip_tags(para,'E')                       
            if para.text is not None:
                para1 = para.text
                while para.text[:1] is "(" :
                    parenText = re.search('\(([^)]+)', para.text)
                    if parenText:
                        parenText = parenText.group(1)
                    
                    if( 'i' in parenText or 'v' in parenText ):
                        level3 += 1     
                    elif ( parenText.islower() ):
                        level1 += 1
                    elif ( parenText.isdigit() ):
                        level2 += 1
                    elif( parenText.isupper() ):
                        level4 += 1  
                    partLength = len(parenText)  
                    para.text = para1[partLength + 2:]
                    para1 = para.text  
                    
                alltext = alltext + para1 
            
        totalSections = level1 + level2 + level3 + level4
        if (totalSections is not 0):
            vertexDepth = ((1*level1) + (2*level2) + (3*level3) + (4*level4))/(level1 + level2 + level3 + level4) 
            
        vals.append(vertexDepth)
        vals.append(alltext)
        return vals
    except:
        print("Error computing VertexDepth. Check text content.")    
        return 0

def getMetrics(paraList):
    vertexDepth = 0
    wordcount=0
    averagecount=0 
    fleschscore = 0
    fleschkincaid=0
    conditionals = 0
    bracketOccurrences=0
    entropy=0
    alltext=''
    wordvals = []
    pattern = re.compile(r'\b(' + r'|'.join(stopwords.words('english')) + r')\b\s*')
    
    vals = getVertexDepth(paraList)
    vertexDepth = str(vals[0])
    alltext = str(vals[1])
        
    bracketOccurrences = getBracketOccurrence(alltext) 
    entropy = calculateEntropy(alltext, range_printable)
    
    alltext = alltext.strip()
    
    if re.search('[a-zA-Z]', alltext):  
        #Remove stopwords
        text = pattern.sub('', alltext)
        wordcount = getWordCount (text)
        averagecount = getAverageWordCount (text)
        
        fleschscore = getFleschScore (alltext) 
        fleschkincaid = getFleschKincaid (alltext)
        conditionals = getConditionals (alltext)
        
    wordvals.append(wordcount)    
    wordvals.append(averagecount)  
    wordvals.append(fleschscore)    
    wordvals.append(fleschkincaid)      
    wordvals.append(bracketOccurrences)       
    wordvals.append(entropy) 
    wordvals.append(conditionals)
    wordvals.append(vertexDepth)
    
    return wordvals

def strip_non_ascii(string):
    if string is not None:
        #Returns string without ASCII characters
        stripped = (c for c in string if 0 < ord(c) < 127)
        return ''.join(stripped)

def filterTitles(title,sectionNum):
    if title == 'title-12':
        tempSect = re.sub('[^0-9]','', sectionNum)
        sectionNum = int(tempSect)

        if((400 <= sectionNum < 500) or (800 <= sectionNum < 900) or (1400 <= sectionNum < 1500) or (sectionNum == 1510) or (sectionNum == 1511) or (1600 <= sectionNum < 1700) or (1800 <= sectionNum < 1900)):
            return True

    #include all of Title 17    
    if title == 'title-17':
        return False

    return False


def outputSections(parts, codeFile):
    mystring=''
    subpartvalue=''
    formulacount=0    
    try:
        for s in parts:
    
            if s.find('./EAR') is not None:
                parttag = strip_non_ascii(s.find('./EAR').text)        
    
            if s.find('./HD') is not None:
                headtag = strip_non_ascii(s.find('./HD').text)
    
            if s.find('./SUBPART/HD') is not None:
                subpartvalue = strip_non_ascii(s.find('./SUBPART/HD').text)
    
            sectiontags = s.findall('.//SECTION')
            '''get subject, sect no and paragraphs sections'''
            for sec in sectiontags:
    
                if sec is not None:
                    sectionno = sec.find('./SECTNO')
                    subject = sec.find('./SUBJECT')
    
                    if subject is None:
                        subject = "NONE"                
                    elif subject is not None:
                        subject = subject.text
                        subject = re.sub(r'([^\s\w]|_)+', '', str(subject))
    
                    '''split the title/volume name for excel file column'''
                    titlelist = codeFile.split("/")
    
                    '''erroring out with a empty secno'''
                    if subject is None:
                        subject='None'
    
                    if titlelist[0] in ['title-12', 'title-31', 'title-17']:
    
                        if sectionno.text is None:
                            '''do nothing'''
                        elif sectionno.text is not None:
                            sectionnum = strip_non_ascii(sectionno.text)  
    
                            if sectionnum != '':
                                checkexclude = filterTitles(titlelist[0], sectionnum)
    
                                if not checkexclude: 
                                    #Vertex Count
                                    pnum = sec.findall('.//P')
                                    
                                    #Table Count
                                    tablenum = sec.findall('.//GPOTABLE')
                                    
                                    #Formula Count
                                    formval = sec.find('./FP')
                                    if formval is not None: 
                                        formulasource = formval.get('SOURCE')
                                        if formulasource =='FP-2':
                                            formulacount = formulacount + 1
                                            
                                    #Call method to receive remaining metrics
                                    wordvals = getMetrics(pnum)
                                    
                                    #Compile all metrics for output to CSV
                                    if not mystring:
                                        mystring = mystring + "*" +  titlelist[0] + "*" + titlelist[1] + "*" + parttag + "*" + subpartvalue + "*" + headtag + "*" + subject + "*" + sectionnum +  "*" + str(len(pnum)) + "*" + str(wordvals[0]) + "*" + str(wordvals[1]) + "*" + str(wordvals[2] ) + "*" + str(wordvals[3] )  + "*" + str(wordvals[4] ) + "*" + str(wordvals[5] ) + "*" + str(len(tablenum)) + "*" + str(formulacount) + "*" + str(wordvals[6]) + "*" + str(wordvals[7])
       
                                    if mystring:
                                        if subpartvalue is None:
                                            subpartvalue = ""
                                        if parttag is None:
                                            parttag = ""
                                        if headtag is None:
                                            headtag = ""
                                        mystring = titlelist[0] + "*" + titlelist[1] + "*" + parttag + "*" + subpartvalue + "*" + headtag + "*" + subject + "*" + sectionnum + "*"  + str(len(pnum)) + "*" + str(wordvals[0]) + "*" + str(wordvals[1]) + "*" + str(wordvals[2]) + "*" + str(wordvals[3] ) + "*" + str(wordvals[4] ) + "*" + str(wordvals[5] ) + "*" + str(len(tablenum)) + "*" + str(formulacount) + "*" + str(wordvals[6]) + "*" + str(wordvals[7])
                                        
                                        if "-" not in sectionnum:
                                            output_rows.append(mystring)
                                            formulacount=0
    except:
        print("Error parsing XML content for sections.")  
        

def parseBuffer(xmlBuffer, codeFile):
    parts = xmlBuffer.findall('.//PART')
    outputSections(parts, codeFile)

if __name__ == "__main__":
    
    #Prompt the User for directory path
    try:
        for i in range(1,5):
            path = input("Please enter the directory path for annual zip files: ")
            if os.path.isdir(path):
                print ("Directory found. Assuming zip files exist in this directory..")
                break
            else:
                print ("Directory does not exist. Please provide a valid directory path containing a zip with xml files.")
    except:
        print("Error in main. The directory or a zip file within the directory was not found.")  
        
    
    #Search for zip folders and xml files within folders
    for file in os.listdir(path):
        if file.endswith((".zip")):    
            codeZip = zipfile.ZipFile(path + '/' + file)
            codeFiles = [codeFile for codeFile in sorted(codeZip.namelist()) if codeFile.lower().endswith('.xml')]
            
            try:
                #Read XML files and Parse
                elements = []
                for codeFile in codeFiles:
                    print ("Currently reading: ", codeFile)
                    xmlBuffer = codeZip.read(codeFile)
                    doc = etree.XML(xmlBuffer)
                    parseBuffer(doc, codeFile)
            except:
                print("Error reading XML file.")              
            
            try:
                #Create CSV file to output data
                filenameparts = file.split(".")
                f = open(path + '/' + filenameparts[0] + '.csv', 'w', encoding='utf-8')
                w = csv.writer(f, delimiter = '*')
                w.writerow(['TITLE', 'FILE NAME', 'PART', 'SUBPART', 'HEADING', 'SUBJECT', 'SECTION', 'VERTICES', 'WORD COUNT', 'AVERAGE WORD LENGTH', 'FLESCH SCORE', 'FLESCH-KINCAID SCORE', 'BRACKET INCIDENCE', 'SHANNON ENTROPY', 'TABLE COUNT', 'FORMULA COUNT', 'CONDITIONALS', 'VERTEX DEPTH'])
                w.writerows([ x.split('*') for x in output_rows])
                f.close()
                print("Successfully wrote the metrics to CSV file with file name " + filenameparts[0] + ".csv") 
                
            except:
                print("Error writing the metrics to CSV file with file name " + filenameparts[0] + ".csv")  
                
            output_rows[:] = []    