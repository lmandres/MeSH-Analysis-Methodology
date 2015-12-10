from datetime import timedelta
from datetime import date

from time import localtime
from time import sleep

import searchlib.eutils
import searchlib.helper
import searchlib.pubmeddb

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
    
    def run_generate_pajek(self):

        fileName = 'actsi_one_mode_-_mesh_terms_-_'
        
        if self.pubmed_database.open_database():
                
            print('Database opened.')

            fromClause = """
                FROM ((((((((
                    tbl_Publication_Mesh_Terms pmt_1 INNER JOIN tbl_Mesh_Descriptor_Tree_Numbers mdtn_1 ON
                        pmt_1.Mesh_Descriptor_ID = mdtn_1.Mesh_Descriptor_ID)
                    INNER JOIN tbl_Mesh_Descriptors tmd_1 ON
                        pmt_1.Mesh_Descriptor_ID = tmd_1.Mesh_Descriptor_ID)
                    INNER JOIN xls_Mesh_Descriptor_Tree_Categories mdtc_1 ON
                        LEFT(mdtn_1.Mesh_Descriptor_Tree_Number, 1) = mdtc_1.Mesh_Descriptor_Tree_Category)
                    INNER JOIN tbl_Publication_Mesh_Terms pmt_2 ON
                        pmt_1.Publication_ID = pmt_2.Publication_ID)
                    INNER JOIN tbl_Mesh_Descriptor_Tree_Numbers mdtn_2 ON
                        pmt_2.Mesh_Descriptor_ID = mdtn_2.Mesh_Descriptor_ID)
                    INNER JOIN tbl_Mesh_Descriptors tmd_2 ON
                        pmt_2.Mesh_Descriptor_ID = tmd_2.Mesh_Descriptor_ID)
                    INNER JOIN xls_Mesh_Descriptor_Tree_Categories mdtc_2 ON
                        LEFT(mdtn_2.Mesh_Descriptor_Tree_Number, 1) = mdtc_2.Mesh_Descriptor_Tree_Category)
                    INNER JOIN tbl_PubMed_Publications pmp_1 ON
                        pmt_1.Publication_ID = pmp_1.Publication_ID)
            """

            yearSQLQuery = """
                SELECT DISTINCT sq_1.Publish_Year
                FROM (
                        SELECT DISTINCT pmp_1.Publish_Year
                        """ + fromClause + """
                        WHERE (mdtn_1.Mesh_Descriptor_ID <> mdtn_2.Mesh_Descriptor_ID)
                        ORDER BY pmp_1.Publish_Year
                    ) sq_1
                WHERE 2007 <= sq_1.Publish_Year AND sq_1.Publish_Year <= 2015
                ORDER BY sq_1.Publish_Year
            """

            vertexSQLQuery = """
                SELECT DISTINCT sq_1.Mesh_Descriptor, sq_1.Mesh_Descriptor_Tree_Category_Description, sq_1.Publication_ID
                FROM (
                        SELECT DISTINCT tmd_1.Mesh_Descriptor, mdtc_1.Mesh_Descriptor_Tree_Category_Description, pmp_1.Publication_ID
                        """ + fromClause + """
                        WHERE (mdtn_1.Mesh_Descriptor_ID <> mdtn_2.Mesh_Descriptor_ID) AND
                            (pmp_1.Publish_Year = ?)
                        GROUP BY tmd_1.Mesh_Descriptor, mdtc_1.Mesh_Descriptor_Tree_Category_Description, pmp_1.Publication_ID
                    ) sq_1
                ORDER BY sq_1.Mesh_Descriptor, sq_1.Mesh_Descriptor_Tree_Category_Description, sq_1.Publication_ID
            """

            edgeSQLQuery = """
                SELECT DISTINCT sq_1.Mesh_Descriptor_1, sq_1.Mesh_Descriptor_Tree_Category_Description_1, sq_1.Mesh_Descriptor_2, sq_1.Mesh_Descriptor_Tree_Category_Description_2, sq_1.Publication_ID
                FROM (
                        SELECT DISTINCT tmd_1.Mesh_Descriptor AS Mesh_Descriptor_1, mdtc_1.Mesh_Descriptor_Tree_Category_Description AS Mesh_Descriptor_Tree_Category_Description_1, tmd_2.Mesh_Descriptor AS Mesh_Descriptor_2, mdtc_2.Mesh_Descriptor_Tree_Category_Description AS Mesh_Descriptor_Tree_Category_Description_2, pmp_1.Publication_ID
                        """ + fromClause + """
                        WHERE (mdtn_1.Mesh_Descriptor_ID < mdtn_2.Mesh_Descriptor_ID) AND
                            (pmp_1.Publish_Year = ?)
                    ) sq_1
                ORDER BY sq_1.Mesh_Descriptor_1, sq_1.Mesh_Descriptor_Tree_Category_Description_1, sq_1.Mesh_Descriptor_2, sq_1.Mesh_Descriptor_Tree_Category_Description_2, sq_1.Publication_ID
            """

            publishYearList = []
            meshDescriptorTreeDescriptionList = []
            meshDescriptorTreeDescriptionList.append(None)
            
            for yearRow in self.pubmed_database.database_manager.run_sql_query(yearSQLQuery):
                publishYearList.append(yearRow.Publish_Year)

            for yearItem in publishYearList:

                print('Publish Year: ' + str(yearItem))
                
                fileOut = open(fileName + str(yearItem) + '.net', 'w')
                vertexCount = 0
                                           
                row = self.pubmed_database.database_manager.database_cursor.execute("""
                    SELECT COUNT(*) AS Vertex_Count
                    FROM (
                    SELECT DISTINCT sq_1.Mesh_Descriptor, sq_1.Mesh_Descriptor_Tree_Category_Description
                    FROM (""" + vertexSQLQuery + """)) AS sq_1_1
                    ;
                """, (str(yearItem))).fetchone()
                
                if row:
                    
                    vertexCount = row.Vertex_Count
                    
                    print('Vertex count = ' + str(vertexCount))
                    fileOut.write('*Vertices ' + str(vertexCount) + '\n')

                rowIndex = 0
                vertexDictionaries = {}
                vertexKeys = []
                
                cursor = self.pubmed_database.database_manager.database_cursor.execute("""
                    SELECT sq_1_1.Mesh_Descriptor, sq_1_1.Mesh_Descriptor_Tree_Category_Description, COUNT(*) AS Vertex_Count
                    FROM (""" + vertexSQLQuery + """) sq_1_1
                    GROUP BY sq_1_1.Mesh_Descriptor, sq_1_1.Mesh_Descriptor_Tree_Category_Description
                    ;
                """, (str(yearItem)))
                print('Writing vertices . . .')
                for row in cursor:

                    rowString =  row.Mesh_Descriptor_Tree_Category_Description.strip() + ' > ' + row.Mesh_Descriptor.strip()
                            
                    rowIndex += 1
                    vertexKeys.append(rowString)
                        
                    vertexDictionaries[rowString] = {}
                    vertexDictionaries[rowString]['vertexIndex'] = rowIndex
                    vertexDictionaries[rowString]['vertexCount'] = row.Vertex_Count
                    vertexDictionaries[rowString]['meshDescriptor'] = row.Mesh_Descriptor.strip()
                    vertexDictionaries[rowString]['meshDescriptorTreeDescription'] = row.Mesh_Descriptor_Tree_Category_Description.strip()
                        
                    fileOut.write(str(vertexDictionaries[rowString]['vertexIndex']) + ' "' + rowString + '"\n')

                fileOut.write('*Arcs\n')
                fileOut.write('*Edges\n')
                                
                cursor = self.pubmed_database.database_manager.database_cursor.execute("""
                    SELECT sq_1_1.Mesh_Descriptor_1, sq_1_1.Mesh_Descriptor_Tree_Category_Description_1, sq_1_1.Mesh_Descriptor_2, sq_1_1.Mesh_Descriptor_Tree_Category_Description_2, COUNT(*) AS Edge_Count
                    FROM (""" + edgeSQLQuery + """) sq_1_1
                    GROUP BY sq_1_1.Mesh_Descriptor_1, sq_1_1.Mesh_Descriptor_Tree_Category_Description_1, sq_1_1.Mesh_Descriptor_2, sq_1_1.Mesh_Descriptor_Tree_Category_Description_2
                    ;
                """, (str(yearItem)))
                print('Writing edges . . .')
                for row in cursor:

                    rowString_1 = row.Mesh_Descriptor_Tree_Category_Description_1.strip() + ' > ' + row.Mesh_Descriptor_1.strip()
                    rowString_2 = row.Mesh_Descriptor_Tree_Category_Description_2.strip() + ' > ' + row.Mesh_Descriptor_2.strip()

                    if rowString_1 and rowString_2:
                        fileOut.write(str(vertexDictionaries[rowString_1]['vertexIndex']) + ' ' + str(vertexDictionaries[rowString_2]['vertexIndex']) + ' ' + str(row.Edge_Count) + '\n')
                        
                fileOut.close()

                fileOut = open(fileName + str(yearItem) + '.clu', 'w')
                fileOut2 = open(fileName + str(yearItem) + '.vec', 'w')
                fileOut3 = open(fileName + str(yearItem) + '.clu_labels', 'w')

                print('Writing clusters and vectors . . .')
                fileOut.write('*Vertices ' + str(vertexCount) + '\n')
                fileOut2.write('*Vertices ' + str(vertexCount) + '\n')
                fileOut3.write('*Vertices ' + str(vertexCount) + '\n')

                for vertexKey in vertexKeys:

                    if vertexDictionaries[vertexKey]['meshDescriptorTreeDescription'] not in meshDescriptorTreeDescriptionList:
                        meshDescriptorTreeDescriptionList.append(vertexDictionaries[vertexKey]['meshDescriptorTreeDescription'])

                    fileOut.write(str(meshDescriptorTreeDescriptionList.index(vertexDictionaries[vertexKey]['meshDescriptorTreeDescription'])) + '\n')
                    fileOut2.write(str(vertexDictionaries[vertexKey]['vertexCount']) + '\n')
                    fileOut3.write(str(meshDescriptorTreeDescriptionList.index(vertexDictionaries[vertexKey]['meshDescriptorTreeDescription'])) + ' ' + vertexDictionaries[vertexKey]['meshDescriptorTreeDescription'] + '\n')

                fileOut.close()
                fileOut2.close()
                fileOut3.close()
            
        if self.pubmed_database.close_database():
            print('Database closed.')
        
        print('Done!')
        
    try:
        if sys.argv[1] == '--license':
            print('''
PubMed Publications Search
Script that creates automatic searches on PubMed.
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
        pubmed_search.run_generate_pajek()
