from datetime import timedelta
from datetime import date

from time import localtime
from time import sleep

import searchlib.eutils
import searchlib.helper
import searchlib.pubmeddb

import re
import sys

class PubMedSearchApp:
    
    search_settings = None
    pubmed_database = None
    search_tool = None
    
    def __init__(self):
    
        self.search_settings = searchlib.helper.PubMedSearchSettings()
        self.pubmed_database = searchlib.pubmeddb.PubMedSearchDatabaseController()
        self.search_tool = searchlib.eutils.EUtilsPubMed()
        
        self.pubmed_database.set_connection_type(self.search_settings.get_database_connection_type())
        self.pubmed_database.set_connection_properties(self.search_settings.get_database_connection_properties())
        
        self.search_tool.set_eutils_address(self.search_settings.get_eutils_address())
        self.search_tool.set_sleep_delay(self.search_settings.get_http_delay())
        self.search_tool.set_maximum_tries(self.search_settings.get_maximum_tries())
        self.search_tool.set_timeout(self.search_settings.get_timeout())
        self.search_tool.set_maximum_url_length(self.search_settings.get_maximum_url_length())
        self.search_tool.set_return_maximum(self.search_settings.get_return_maximum())
        self.search_tool.set_eutils_use_history(self.search_settings.get_eutils_use_history())
        self.search_tool.set_email_address(self.search_settings.get_email_address())
        self.search_tool.set_tool_name(self.search_settings.get_search_tool_name())
	
    def run_mesh_update(self):
        
        if self.pubmed_database.open_database():
                
            print('Database opened.')

            xmlReadStage = 1
            xmlString = ''
            xmlStringData = ''
                
            if self.search_settings.get_database_connection_type().upper() == 'SQLITE3DRIVER':
                    
                import sqlite3
                    
                try:
                    self.pubmed_database.database_manager.database_cursor.execute("DROP TABLE tbl_Mesh_Descriptor_Tree_Numbers;")
                    self.pubmed_database.database_manager.database_connection.commit()
                    print('Dropped table: tbl_Mesh_Descriptor_Tree_Numbers')
                except sqlite3.OperationalError as oer:
                    if oer.args[0][:14] == 'no such table:':
                        print('Drop table skipped: tbl_Mesh_Descriptor_Tree_Numbers')
                    else:
                        raise oer
                    
                try:
                    self.pubmed_database.database_manager.database_cursor.execute("""
                                                    CREATE TABLE tbl_Mesh_Descriptor_Tree_Numbers (
                                                        Mesh_Descriptor_Tree_Number_ID INTEGER PRIMARY KEY,
                                                        Mesh_Descriptor_ID INTEGER,
                                                        Mesh_Descriptor_Tree_Number TEXT
                                                    );""")
                    self.pubmed_database.database_manager.database_connection.commit()
                    print('Created table: tbl_Mesh_Descriptor_Tree_Numbers')
                except sqlite3.ProgrammingError as per:
                    if per.args[0] == '42S01':
                        print('Create table skipped: tbl_Mesh_Descriptor_Tree_Numbers')
                    else:
                        raise per
                    
                try:
                    
                    self.pubmed_database.database_manager.database_cursor.execute("""
                                                    CREATE TABLE tbl_Mesh_Descriptor_Tree_Categories (
                                                        Mesh_Descriptor_Tree_Category_ID INTEGER PRIMARY KEY,
                                                        Mesh_Descriptor_Tree_Category TEXT,
                                                        Mesh_Descriptor_Tree_Category_Description TEXT
                                                    );""")
                    self.pubmed_database.database_manager.database_connection.commit()
                    print('Created table: tbl_Mesh_Descriptor_Tree_Categories')

                    fileIn = open('meshDescriptorTreeCategories.txt', 'r')
                    for fileLine in fileIn:
                        
                        lineArray = fileLine.strip().split('\t')
                        self.pubmed_database.database_manager.database_cursor.execute("INSERT INTO tbl_Mesh_Descriptor_Tree_Categories (Mesh_Descriptor_Tree_Category, Mesh_Descriptor_Tree_Category_Description) VALUES (?, ?);",
                            (lineArray[0].strip(),
                             lineArray[1].strip()))
                        self.pubmed_database.database_manager.database_connection.commit()
                            
                    fileIn.close()
                        
                except sqlite3.ProgrammingError as per:
                    if per.args[0] == '42S01':
                        print('Create table skipped: tbl_Mesh_Descriptor_Tree_Categories')
                    else:
                        raise per
                except sqlite3.OperationalError as oer:
                    print('Create table skipped: tbl_Mesh_Descriptor_Tree_Categories')

            elif self.search_settings.get_database_connection_type().upper() == 'PYODBCDRIVER':

                import pyodbc
      
                try:
                    self.pubmed_database.database_manager.database_cursor.execute("DROP TABLE tbl_Mesh_Descriptor_Tree_Numbers;")
                    self.pubmed_database.database_manager.database_cursor.commit()
                    print('Dropped table: tbl_Mesh_Descriptor_Tree_Numbers')
                except pyodbc.ProgrammingError as per:
                    if per.args[0] == '42S02':
                        print('Drop table skipped: tbl_Mesh_Descriptor_Tree_Numbers')
                    else:
                        raise per
                except pyodbc.Error as er:
                    if er.args[0] == 'HY109':
                        print('Drop table skipped: tbl_Mesh_Descriptor_Tree_Numbers')
                    else:
                        raise er
     
                try:
                    self.pubmed_database.database_manager.database_cursor.execute("""
                                                            CREATE TABLE tbl_Mesh_Descriptor_Tree_Numbers (
                                                                Mesh_Descriptor_Tree_Number_ID COUNTER PRIMARY KEY,
                                                                Mesh_Descriptor_ID INTEGER,
                                                                Mesh_Descriptor_Tree_Number TEXT
                                                            );""")
                    self.pubmed_database.database_manager.database_cursor.commit()
                    print('Created table: tbl_Mesh_Descriptor_Tree_Numbers')
                except pyodbc.ProgrammingError as per:
                    if per.args[0] == '42S01':
                        print('Create table skipped: tbl_Mesh_Descriptor_Tree_Numbers')
                    else:
                        raise per
                    
                try:
                    
                    self.pubmed_database.database_manager.database_cursor.execute("""
                                                    CREATE TABLE tbl_Mesh_Descriptor_Tree_Categories (
                                                        Mesh_Descriptor_Tree_Category_ID COUNTER PRIMARY KEY,
                                                        Mesh_Descriptor_Tree_Category TEXT,
                                                        Mesh_Descriptor_Tree_Category_Description TEXT
                                                    );""")
                    self.pubmed_database.database_manager.database_cursor.commit()
                    print('Created table: tbl_Mesh_Descriptor_Tree_Categories')

                    fileIn = open('meshDescriptorTreeCategories.txt', 'r')
                    for fileLine in fileIn:
                        
                        lineArray = fileLine.strip().split('\t')
                        self.pubmed_database.database_manager.database_cursor.execute("INSERT INTO tbl_Mesh_Descriptor_Tree_Categories (Mesh_Descriptor_Tree_Category, Mesh_Descriptor_Tree_Category_Description) VALUES (?, ?);",
                            (lineArray[0].strip(),
                            lineArray[1].strip()))
                        self.pubmed_database.database_manager.database_cursor.commit()
                            
                    fileIn.close()
                        
                except pyodbc.ProgrammingError as per:
                    if per.args[0] == '42S01':
                        print('Create table skipped: tbl_Mesh_Descriptor_Tree_Categories')
                    else:
                        raise per
            
            xmlObject = searchlib.helper.TextXMLParser()
                                        
            xmlOpenRegEx = re.compile('(?!<DescriptorRecordSet.*?>)(<DescriptorRecord.*?>.*)')
            xmlCloseRegEx = re.compile('(.*</DescriptorRecord>)')
            
            fileIn = open(self.search_settings.search_settings.get_string_cdata('<BiblioAnalysisSettings><ClassifyMeSHTermsSettings><MeSHDescriptorsFileName>').strip(), 'r')   
            for fileLine in fileIn:
                
                if xmlReadStage == 1:
                            
                    match = xmlOpenRegEx.search(fileLine)
                    if match:
                        xmlString = match.group(1)
                        xmlReadStage += 1
                        
                elif xmlReadStage == 2:
                
                    match = xmlCloseRegEx.search(fileLine)
                    if not match:
                        xmlString += fileLine
                    else:
                                
                        xmlString += match.group(1)
                                                
                        xmlObject.parse_xml_string(xmlString.encode('utf-8'))
                        xmlStringData = xmlString
                
                        print(
                            xmlObject.get_element_item('<DescriptorRecord><DescriptorUI>')['character_data'][0].strip() + ' = ' +
                            xmlObject.get_element_item('<DescriptorRecord><DescriptorName><String>')['character_data'][0].strip())
                
                        rows = self.pubmed_database.database_manager.database_cursor.execute("SELECT Mesh_Descriptor_ID FROM tbl_Mesh_Descriptors WHERE Mesh_Descriptor = ?;", ((
                            xmlObject.get_element_item('<DescriptorRecord><DescriptorName><String>')['character_data'][0].strip()),)).fetchall()

                        if len(rows):
                
                            for row in rows:
                
                                if row[0]:
                                            
                                    treeNumberIndex = 0
                                            
                                    self.pubmed_database.database_manager.database_cursor.execute("UPDATE tbl_Mesh_Descriptors SET DescriptorUI = ? WHERE Mesh_Descriptor_ID = ?;", (
                                        (xmlObject.get_element_item('<DescriptorRecord><DescriptorUI>')['character_data'][0].strip(),
                                        str(row[0]))))
                                        
                                    if self.search_settings.get_database_connection_type().upper() == 'SQLITE3DRIVER':
                                        self.pubmed_database.database_manager.database_cursor.commit()
                                    elif self.search_settings.get_database_connection_type().upper() == 'PYODBCDRIVER':
                                        self.pubmed_database.database_manager.database_connection.commit()
                
                                    while xmlObject.get_element_item('<DescriptorRecord><TreeNumberList><TreeNumber<' + str(treeNumberIndex).strip() + '>>'):
                                            
                                        self.pubmed_database.database_manager.database_cursor.execute("INSERT INTO tbl_Mesh_Descriptor_Tree_Numbers (Mesh_Descriptor_ID, Mesh_Descriptor_Tree_Number) VALUES (?, ?);", (
                                            (str(row[0]),
                                            xmlObject.get_element_item('<DescriptorRecord><TreeNumberList><TreeNumber<' + str(treeNumberIndex).strip() + '>>')['character_data'][0].strip()))) 
                                        
                                        if self.search_settings.get_database_connection_type().upper() == 'SQLITE3DRIVER':
                                            self.pubmed_database.database_manager.database_cursor.commit()
                                        elif self.search_settings.get_database_connection_type().upper() == 'PYODBCDRIVER':
                                            self.pubmed_database.database_manager.database_connection.commit()

                                        treeNumberIndex += 1
                                            
                        xmlReadStage = 1
                
            fileIn.close()
            
        if self.pubmed_database.close_database():
            print('Closed database.')

        print('Done!')

if __name__ == '__main__':
    
    try:
        if sys.argv[1] == '--license':
            print('''
update_mesh_descriptors.py
Script that updates MeSH Descriptors in PubMed Search Database.
Copyright (C) 2015  Leo Andres

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Leo Andres
P. O. Box 3174
Decatur, GA 30031
lmandres@yahoo.com\n''')
    except IndexError as ie:
        pubmed_search = PubMedSearchApp()
        pubmed_search.run_mesh_update()
