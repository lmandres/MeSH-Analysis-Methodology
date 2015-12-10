import os
import re
import shutil
import sqlite3

class PubMedSQLite3DatabaseController:
    '''
    classdocs
    '''

    connection_string = None

    database_connection = None
    database_cursor = None
    
    def open_database(self, odbc_connection_string_in):

        self.connection_string = odbc_connection_string_in
        
        try:

            self.database_connection = sqlite3.connect(self.connection_string)
            self.database_connection.row_factory = sqlite3.Row
            
            self.database_cursor = self.database_connection.cursor()

            return True

        except TypeError:

            self.database_connection = None
            return False

        except sqlite3.Error as er:
            
            if er.args[0] == 'HY000':
                self.database_connection = None
                return False
            else:
                raise er
        
    def close_database(self):
        
        try:

            self.database_cursor.close()
            self.database_connection.close()

            self.database_cursor = None
            self.database_connection = None

            return True
        
        except AttributeError:
            
            self.database_cursor = None
            self.database_connection = None
            return False
    
    def run_sql_query(self, sql_query_in):
        return self.database_cursor.execute(sql_query_in)

class PubMedSQLite3DatabaseSearch(PubMedSQLite3DatabaseController):
    
    PERSON_BY_PERSON = 2**0
    PERSON_ORGANIZATION = 2**1
    PERSON_GRANT = 2**2
    PERSON_COAUTHOR = 2**3
    CTSA_GRANT = 2**4
    PMCID_CITE_BY_PMCID = 2**5
    PUBMED_ID_CITE_BY_PUBMED_ID = 2**6
    PUBMED_ID_NEIGHBOR_PUBMED_ID = 2**7
    
    search_method_query_parts = {
                                 PERSON_BY_PERSON : {
                                                      'selectFields' : """
                                                                          sq_1.PERSON_ID AS Search_Person_ID,
                                                                          CASE
                                                                              WHEN sq_1.FAMILY_NAME IS NOT NULL THEN trim(sq_1.FAMILY_NAME)
                                                                              ELSE ''
                                                                          END AS Search_Person_Last_Name,
                                                                          CASE
                                                                              WHEN sq_1.FIRST_GIVEN_NAME IS NOT NULL THEN trim(sq_1.FIRST_GIVEN_NAME)
                                                                              ELSE ''
                                                                          END AS Search_Person_First_Name,
                                                                          CASE
                                                                              WHEN sq_1.SECOND_GIVEN_NAME IS NOT NULL THEN substr(sq_1.SECOND_GIVEN_NAME, 1, 1)
                                                                              ELSE ''
                                                                          END AS Search_Person_Middle_Name,
                                                                          
                                                                          sq_2.PERSON_ID AS Co_Investigator_ID,
                                                                          CASE
                                                                              WHEN sq_2.FAMILY_NAME IS NOT NULL THEN trim(sq_2.FAMILY_NAME)
                                                                              ELSE ''
                                                                          END AS Co_Investigator_Last_Name,
                                                                          CASE
                                                                              WHEN sq_2.FIRST_GIVEN_NAME IS NOT NULL THEN trim(sq_2.FIRST_GIVEN_NAME)
                                                                              ELSE ''
                                                                          END AS Co_Investigator_First_Name,
                                                                          CASE
                                                                              WHEN sq_2.SECOND_GIVEN_NAME IS NOT NULL THEN substr(sq_2.SECOND_GIVEN_NAME, 1, 1)
                                                                              ELSE ''
                                                                          END AS Co_Investigator_Middle_Name""",
                                                       'fromClause' : """
                                                                         FROM
                                                                             (
                                                                                 SELECT DISTINCT
                                                                                    sp_1_1.PERSON_ID,
                                                                                    sp_1_1.FAMILY_NAME,
                                                                                    sp_1_1.FIRST_GIVEN_NAME,
                                                                                    sp_1_1.SECOND_GIVEN_NAME
                                                                                FROM tbl_Search_Persons sp_1_1
                                                                            ) sq_1 INNER JOIN (
                                                                                 SELECT DISTINCT
                                                                                    sp_1_2.PERSON_ID,
                                                                                    sp_1_2.FAMILY_NAME,
                                                                                    sp_1_2.FIRST_GIVEN_NAME,
                                                                                    sp_1_2.SECOND_GIVEN_NAME
                                                                                FROM tbl_Search_Persons sp_1_2
                                                                            ) sq_2 ON
                                                                                sq_1.PERSON_ID < sq_2.PERSON_ID""",
                                                       'orderByClause' : """
                                                                            ORDER BY
                                                                                sq_1.FAMILY_NAME,
                                                                                sq_1.FIRST_GIVEN_NAME,
                                                                                sq_1.SECOND_GIVEN_NAME,
                                                                                sq_2.FAMILY_NAME,
                                                                                sq_2.FIRST_GIVEN_NAME,
                                                                                sq_2.SECOND_GIVEN_NAME"""},
                                 
                                 PERSON_ORGANIZATION : {
                                                         'selectFields' : """
                                                                             sq_1_1.PERSON_ID AS Search_Person_ID,
                                                                              CASE
                                                                                  WHEN sq_1_1.FAMILY_NAME IS NOT NULL THEN trim(sq_1_1.FAMILY_NAME)
                                                                                  ELSE ''
                                                                              END AS Search_Person_Last_Name,
                                                                              CASE
                                                                                  WHEN sq_1_1.FIRST_GIVEN_NAME IS NOT NULL THEN trim(sq_1_1.FIRST_GIVEN_NAME)
                                                                                  ELSE ''
                                                                              END AS Search_Person_First_Name,
                                                                              CASE
                                                                                  WHEN sq_1_1.SECOND_GIVEN_NAME IS NOT NULL THEN substr(sq_1_1.SECOND_GIVEN_NAME, 1, 1)
                                                                                  ELSE ''
                                                                              END AS Search_Person_Middle_Name,
                                                                             trim(aol_1_1.ACTSI_ORGANIZATION_NAME) AS ACTSI_Organization""",
                                                         'fromClause' : """
                                                                           FROM
                                                                               (
                                                                                 SELECT DISTINCT
                                                                                    sp_1_1_1.PERSON_ID,
                                                                                    sp_1_1_1.FAMILY_NAME,
                                                                                    sp_1_1_1.FIRST_GIVEN_NAME,
                                                                                    sp_1_1_1.SECOND_GIVEN_NAME
                                                                                FROM tbl_Search_Persons sp_1_1_1
                                                                               )  AS sq_1_1, tbl_Search_Organizations aol_1_1
                                                                           WHERE aol_1_1.ACTSI_ORGANIZATION_ID <> 13""",
                                                         'orderByClause' : """
                                                                              ORDER BY
                                                                                  sq_1_1.FAMILY_NAME,
                                                                                  sq_1_1.FIRST_GIVEN_NAME,
                                                                                  sq_1_1.SECOND_GIVEN_NAME,
                                                                                  Trim(aol_1_1.ACTSI_ORGANIZATION_NAME)"""},
                                 
                                 PERSON_GRANT : {
                                                  'selectFields' : """
                                                                      sq_1.PERSON_ID AS Search_Person_ID,
                                                                      CASE
                                                                          WHEN sq_1.FAMILY_NAME IS NOT NULL THEN trim(sq_1.FAMILY_NAME)
                                                                          ELSE ''
                                                                      END AS Search_Person_Last_Name,
                                                                      CASE
                                                                          WHEN sq_1.FIRST_GIVEN_NAME IS NOT NULL THEN trim(sq_1.FIRST_GIVEN_NAME)
                                                                          ELSE ''
                                                                      END AS Search_Person_First_Name,
                                                                      CASE
                                                                          WHEN sq_1.SECOND_GIVEN_NAME IS NOT NULL THEN substr(sq_1.SECOND_GIVEN_NAME, 1, 1)
                                                                          ELSE ''
                                                                      END AS Search_Person_Middle_Name,
                                                                      sq_1.Research_Grant_ID,
                                                                      sq_1.PHS_Organization + sq_1.PHS_Six_Digit_Grant_Number AS Grant_Number""",
                                                  'fromClause' : """
                                                                    FROM (
                                                                            SELECT DISTINCT
                                                                                sq_1_1.PERSON_ID,
                                                                                sq_1_1.FAMILY_NAME,
                                                                                sq_1_1.FIRST_GIVEN_NAME,
                                                                                sq_1_1.SECOND_GIVEN_NAME,
                                                                                rg_1_1.Research_Grant_ID,
                                                                                Iif(fol_1_1.FUNDING_ORG_ABBREVIATION Is Not Null, Trim(fol_1_1.FUNDING_ORG_ABBREVIATION), '') AS PHS_Organization,
                                                                                Iif(rg_1_1.GRANT_NUMBER Is Not Null, Format(rg_1_1.GRANT_NUMBER, '000000'), '') AS PHS_Six_Digit_Grant_Number
                                                                            FROM ((
                                                                                    SELECT DISTINCT
                                                                                        sp_1_1_1.PERSON_ID,
                                                                                        sp_1_1_1.FAMILY_NAME,
                                                                                        sp_1_1_1.FIRST_GIVEN_NAME,
                                                                                        sp_1_1_1.SECOND_GIVEN_NAME
                                                                                    FROM tbl_Search_Persons sp_1_1_1
                                                                                ) AS sq_1_1 INNER JOIN (
                                                                                    SELECT DISTINCT
                                                                                        sg_1_1_1.PERSON_ID,
                                                                                        sg_1_1_1.Research_Grant_ID,
                                                                                        sg_1_1_1.PHS_Organization,
                                                                                        sg_1_1_1.PHS_Six_Digit_Grant_Number
                                                                                    FROM tbl_Search_Grants sg_1_1_1
                                                                                ) AS sq_1_2 ON
                                                                                    sq_1_1.PERSON_ID = sq_1_2.PERSON_ID)
                                                                        ) AS sq_1""",
                                                  'orderByClause' : """
                                                                       ORDER BY
                                                                           sq_1.PHS_Organization,
                                                                           sq_1.PHS_Six_Digit_Grant_Number,
                                                                           sq_1.PERSON_ID"""},
                                 
                                 PERSON_COAUTHOR : {
                                                     'selectFields' : """
                                                                         sq_1.Search_Person_ID,
                                                                         sq_1.Search_Person_Last_Name,
                                                                         sq_1.Search_Person_First_Name,
                                                                         sq_1.Search_Person_Middle_Name,
                                                                         sq_1.Coauthor_ID,
                                                                         sq_1.Coauthor_Last_Name,
                                                                         sq_1.Coauthor_First_Name,
                                                                         sq_1.Coauthor_Initials""",
                                                     'fromClause' : """
                                                                       FROM ((
                                                                               SELECT DISTINCT
                                                                                   pr_1_1.PERSON_ID AS Search_Person_ID,
                                                                                      CASE
                                                                                          WHEN pr_1_1.FAMILY_NAME IS NOT NULL THEN trim(pr_1_1.FAMILY_NAME)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_Last_Name,
                                                                                      CASE
                                                                                          WHEN pr_1_1.FIRST_GIVEN_NAME IS NOT NULL THEN trim(pr_1_1.FIRST_GIVEN_NAME)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_First_Name,
                                                                                      CASE
                                                                                          WHEN pr_1_1.SECOND_GIVEN_NAME IS NOT NULL THEN substr(pr_1_1.SECOND_GIVEN_NAME, 1, 1)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_Middle_Name,
                                                                                      
                                                                                   ca_1_1.Coauthor_ID,
                                                                                   CASE
                                                                                       WHEN ca_1_1.LastName IS NOT NULL THEN trim(ca_1_1.LastName)
                                                                                       ELSE ''
                                                                                   END AS Coauthor_Last_Name,
                                                                                   CASE
                                                                                       WHEN ca_1_1.FirstName IS NOT NULL THEN trim(ca_1_1.FirstName)
                                                                                       ELSE ''
                                                                                   END AS Coauthor_First_Name,
                                                                                   CASE
                                                                                       WHEN ca_1_1.Initials IS NOT NULL THEN trim(ca_1_1.Initials)
                                                                                       ELSE ''
                                                                                   END AS Coauthor_Initials
                                                                               FROM ((((
                                                                                        SELECT DISTINCT
                                                                                            sp_1_1_1.PERSON_ID,
                                                                                            sp_1_1_1.FAMILY_NAME,
                                                                                            sp_1_1_1.FIRST_GIVEN_NAME,
                                                                                            sp_1_1_1.SECOND_GIVEN_NAME
                                                                                        FROM tbl_Search_Persons sp_1_1_1
                                                                                   ) pr_1_1 INNER JOIN tbl_Investigator_Publications ip_1_1 ON
                                                                                       pr_1_1.PERSON_ID = ip_1_1.Investigator_ID)
                                                                                   INNER JOIN tbl_Publication_Coauthors pca_1_1 ON
                                                                                       ip_1_1.Publication_ID = pca_1_1.Publication_ID)
                                                                                   INNER JOIN tbl_Coauthors ca_1_1 ON
                                                                                       pca_1_1.Coauthor_ID = ca_1_1.Coauthor_ID)
                                                                                WHERE (trim(ca_1_1.LastName) NOT LIKE '%' + trim(pr_1_1.FAMILY_NAME) + '%' AND
                                                                                        substr(trim(ca_1_1.FirstName), 1, 1) <> substr(trim(pr_1_1.FIRST_GIVEN_NAME), 1, 1)) OR
                                                                                        (trim(pr_1_1.FAMILY_NAME) NOT LIKE '%' + trim(ca_1_1.LastName) + '%' AND
                                                                                        substr(trim(pr_1_1.FIRST_GIVEN_NAME), 1, 1) <> substr(trim(ca_1_1.FirstName), 1, 1))
                                                                           ) AS sq_1 LEFT OUTER JOIN tbl_Investigator_Coauthors_Searched ics_1 ON
                                                                               sq_1.Search_Person_ID = ics_1.Investigator_ID AND
                                                                               sq_1.Coauthor_ID = ics_1.Coauthor_ID)
                                                                       WHERE ics_1.Investigator_Coauthor_ID IS NULL""",
                                                     'orderByClause' : """
                                                                          ORDER BY
                                                                              sq_1.Search_Person_Last_Name,
                                                                              sq_1.Search_Person_First_Name,
                                                                              sq_1.Search_Person_Middle_Name,
                                                                              sq_1.Coauthor_Last_Name,
                                                                              sq_1.Coauthor_First_Name,
                                                                              sq_1.Coauthor_Initials"""},
                                 
                                 CTSA_GRANT : {
                                                     'selectFields' : """
                                                                         gs_1.CTSA_Grant_Number_ID,
                                                                         gs_1.CTSA_Grant_Number""",
                                                     'fromClause' : """
                                                                       FROM tbl_CTSA_Grant_Searches gs_1""",
                                                     'orderByClause' : """
                                                                          ORDER BY
                                                                                  gs_1.CTSA_Grant_Number_ID,
                                                                                  gs_1.CTSA_Grant_Number"""}}

    search_terms_resultset = None

    current_person_id = None
    current_coinvestigator_id = None
    current_coauthor_id = None
    current_ctsa_grant_id = None

    current_running_flag = None
    
    database_search_filename = None
    
    
    def open_database(self, odbc_connection_string_in):
        
        search_build_filename = odbc_connection_string_in
        
        match = re.match('(.*?)(\..*)', odbc_connection_string_in)
        if match:
            
            search_build_filename = match.group(1).strip() + '_search'
            
            try:
                search_build_filename += match.group(2).strip()
            except AttributeError:
                pass
            except ValueError:
                pass
            
        self.database_search_filename = search_build_filename
        shutil.copy(odbc_connection_string_in, self.database_search_filename)
        
        return PubMedSQLite3DatabaseController.open_database(self, self.database_search_filename)
        
    def close_database(self):
        
        os.remove(self.database_search_filename)

    def get_search_recordset(self, search_strategy_in):
        
        return self.database_cursor.execute(
                                            "SELECT " +
                                            self.search_method_query_parts[search_strategy_in]['selectFields'] + " " +
                                            self.search_method_query_parts[search_strategy_in]['fromClause'] + " " +
                                            self.search_method_query_parts[search_strategy_in]['orderByClause'] + ";")
    
    def get_search_recordcount(self, search_strategy_in):
        
        return_recordcount = 0
        
        count_recordset = self.run_sql_query(
                                             "SELECT COUNT(*) AS Search_Term_Count " +
                                             self.search_method_query_parts[search_strategy_in]['fromClause'] + ";")
        
        row = count_recordset.fetchone()
        return_recordcount = int(row['Search_Term_Count'])
        count_recordset.close()
        
        return return_recordcount
        
    def get_search_term(self):
        
        return_search_term = None
        
        if self.search_terms_resultset == None:
            
            if self.current_running_flag == self.CTSA_GRANT:
            
                try:
                    self.database_cursor.execute("DROP TABLE tbl_CTSA_Grant_Searches;")
                    self.database_connection.commit()
                    print('Dropped table: tbl_CTSA_Grant_Searches')
                except sqlite3.OperationalError as oer:
                    if oer.args[0][:14] == 'no such table:':
                        print('Drop table skipped: tbl_CTSA_Grant_Searches')
                    else:
                        raise oer
                    
                try:
                    self.database_cursor.execute("""
                                                    CREATE TABLE tbl_CTSA_Grant_Searches (
                                                        CTSA_Grant_Search_ID INTEGER PRIMARY KEY,
                                                        CTSA_Grant_Number_ID INTEGER,
                                                        CTSA_Grant_Number TEXT
                                                    );""")
                    self.database_connection.commit()
                    print('Created table: tbl_CTSA_Grant_Searches')
                except sqlite3.ProgrammingError as per:
                    if per.args[0] == '42S01':
                        print('Create table skipped: tbl_CTSA_Grant_Searches')
                    else:
                        raise per
                               
                sql_insert_query = """
                                        INSERT INTO tbl_CTSA_Grant_Searches (
                                            CTSA_Grant_Number_ID,
                                            CTSA_Grant_Number
                                        ) VAlUES (
                                            ?, ?
                                        );"""
                
                sql_select_query = """
                                        SELECT DISTINCT gn_1.CTSA_Grant_Number_ID, gn_1.PHS_Activity_Code, gn_1.PHS_Organization, gn_1.PHS_Six_Digit_Grant_Number
                                        FROM tbl_CTSA_Grant_Numbers AS gn_1
                                        ORDER BY gn_1.PHS_Organization, gn_1.PHS_Six_Digit_Grant_Number
                                        ;"""
                
                for row in self.database_cursor.execute(sql_select_query).fetchall():
                    
                    phs_grant_number = row['PHS_Six_Digit_Grant_Number'].lstrip('0').strip()
                    
                    for number_index in range(0, 6-len(phs_grant_number)+1, 1):
                        
                        phs_edited_grant_number = row['PHS_Organization'].strip() + ("0" * number_index) + phs_grant_number
                        self.database_cursor.execute(sql_insert_query, (int(row['CTSA_Grant_Number_ID']), phs_edited_grant_number))
                        self.database_connection.commit()
                        
                        print('Inserted Search Grant Number: ' + phs_edited_grant_number)
                        
                        phs_edited_grant_number = row['PHS_Activity_Code'].strip() + row['PHS_Organization'].strip() + ("0" * number_index) + phs_grant_number
                        self.database_cursor.execute(sql_insert_query, (int(row['CTSA_Grant_Number_ID']), phs_edited_grant_number))
                        self.database_connection.commit()
                        
                        print('Inserted Search Grant Number: ' + phs_edited_grant_number)
                        
        
            self.search_terms_resultset = self.get_search_recordset(self.current_running_flag)
            
        row = self.search_terms_resultset.fetchone()
        if row:
                        
            running_flag_case = self.current_running_flag                    
            if running_flag_case == self.PERSON_BY_PERSON:
                    
                search_person_term = None
                co_investigator_term = None
                    
                self.current_person_id = int(row['Search_Person_ID'])
                self.current_coinvestigator_id = int(row['Co_Investigator_ID'])
                self.current_coauthor_id = None

                
                search_person_term = row['Search_Person_Last_Name'].strip()
                if row['Search_Person_First_Name'].strip() != '':
                    search_person_term = search_person_term +  ', ' + row['Search_Person_First_Name'].strip()
                    if row['Search_Person_Middle_Name'].strip() != '':
                        search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                elif row['Search_Person_Middle_Name'].strip() != '':
                    search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                
                co_investigator_term = row['Co_Investigator_Last_Name'].strip()
                if row['Co_Investigator_First_Name'].strip() != '':
                    co_investigator_term = co_investigator_term + ', ' + row['Co_Investigator_First_Name'].strip()
                    if row['Co_Investigator_Middle_Name'].strip() != '':
                        co_investigator_term = co_investigator_term + ' ' + row['Co_Investigator_Middle_Name'].strip()
                elif row['Co_Investigator_Middle_Name'].strip() != '':
                    co_investigator_term = co_investigator_term + ' ' + row['Co_Investigator_Middle_Name'].strip()
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + co_investigator_term.strip() + '[au])'
                    
            elif running_flag_case == self.PERSON_ORGANIZATION:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = int(row['Search_Person_ID'])
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = row['Search_Person_Last_Name'].strip()
                if row['Search_Person_First_Name'].strip() != '':
                    search_person_term = search_person_term +  ', ' + row['Search_Person_First_Name'].strip()
                    if row['Search_Person_Middle_Name'].strip() != '':
                        search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                elif row['Search_Person_Middle_Name'].strip() != '':
                    search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                    
                organization_term = row['ACTSI_Organization'].strip()
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + organization_term.strip() + '[ad])'
                    
            elif running_flag_case == self.PERSON_GRANT:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = int(row.Search_Person_ID)
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = row['Search_Person_Last_Name'].strip()
                if row['Search_Person_First_Name'].strip() != '':
                    search_person_term = search_person_term +  ', ' + row['Search_Person_First_Name'].strip()
                    if row['Search_Person_Middle_Name'].strip() != '':
                        search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                elif row['Search_Person_Middle_Name'].strip() != '':
                    search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                    
                return_search_term = ('(' + search_person_term.strip() + '[au]) and (' + row['Grant_Number'].strip() + '[gr])')
                    
            elif running_flag_case == self.PERSON_COAUTHOR:
                    
                search_person_term = None
                co_author_term = None
                    
                co_author_last_name = None
                    
                self.current_person_id = int(row.Search_Person_ID)
                self.current_coinvestigator_id = None
                self.current_coauthor_id = int(row.Coauthor_ID)
                    
                search_person_term = row['Search_Person_Last_Name'].strip()
                if row['Search_Person_First_Name'].strip() != '':
                    search_person_term = search_person_term +  ', ' + row['Search_Person_First_Name'].strip()
                    if row['Search_Person_Middle_Name'].strip() != '':
                        search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                elif row['Search_Person_Middle_Name'].strip() != '':
                    search_person_term = search_person_term +  ' ' + row['Search_Person_Middle_Name'].strip()
                    
                co_author_last_name = row['Coauthor_Last_Name'].strip()
                    
                co_author_term = (
                                  '(' +
                                  co_author_last_name.strip() + ', ' +
                                  row['Coauthor_First_Name'].strip() + '[au]) and (' +
                                  co_author_last_name.strip() + ' ' +
                                  row['Coauthor_Initials'].strip() + '[au])')
                    
                return_search_term = '(' + co_author_term.strip() + ') and (' + search_person_term.strip() + '[au])'
                
            elif running_flag_case == self.CTSA_GRANT:
                    
                self.current_person_id = None
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                
                self.current_ctsa_grant_id = int(row['CTSA_Grant_Number_ID'])
                
                return_search_term = '(' + row['CTSA_Grant_Number'].strip() + '[gr])'
                    
        else:
            self.search_terms_resultset = None
            
        return return_search_term

    def get_person_id(self):
        return self.current_person_id

    def get_coinvestigator_id(self):
        return self.current_coinvestigator_id

    def get_coauthor_id(self):
        return self.current_coauthor_id
    
    def get_ctsa_grant_id(self):
        return self.current_ctsa_grant_id

    def set_running_flag(self, search_strategy_in):
        self.current_running_flag = search_strategy_in

    def get_running_flag(self):
        return self.current_running_flag

class PubMedSQLite3DatabaseManager(PubMedSQLite3DatabaseController):
    
    database_table_parts = [
                            {
                             'tableName' : 'tbl_PubMed_Publications',
                             'tableColumns' : """
                                                 Publication_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 PubMed_ID TEXT,
                                                 Journal TEXT,
                                                 PMCID TEXT,
                                                 Publish_Month INTEGER,
                                                 Publish_Year INTEGER,
                                                 Medline_Date TEXT,
                                                 Authors TEXT,
                                                 Title TEXT,
                                                 Affiliation TEXT,
                                                 Volume TEXT,
                                                 Issue_Number TEXT,
                                                 Pages TEXT,
                                                 Author_List_Complete INTEGER,
                                                 Grant_List_Complete INTEGER,
                                                 PubMed_XML BLOB,
                                                 PubMed_XML_Date TEXT"""},
                            {
                             'tableName' : 'tbl_Investigator_Publications',
                             'tableColumns' : """
                                                 Investigator_Publications_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Investigator_ID INTEGER,
                                                 Publication_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Grants',
                             'tableColumns' : """
                                                 Grant_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Grant_Number TEXT,
                                                 PHS_Activity_Code TEXT,
                                                 PHS_Organization TEXT,
                                                 PHS_Six_Digit_Grant_Number TEXT,
                                                 Acronym TEXT,
                                                 Agency TEXT,
                                                 Country TEXT,
                                                 CTSA_Grant_Number_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Publication_Grants_Cited',
                             'tableColumns' : """
                                                 Publication_Grants_Cited_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Publication_ID INTEGER,
                                                 Grant_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Coauthors',
                             'tableColumns' : """
                                                 Coauthor_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 LastName TEXT,
                                                 FirstName TEXT,
                                                 Initials TEXT,
                                                 Affiliation TEXT"""},
                            {
                             'tableName' : 'tbl_Publication_Coauthors',
                             'tableColumns' : """
                                                 Publication_Coauthor_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Publication_ID INTEGER,
                                                 Coauthor_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Mesh_Descriptors',
                             'tableColumns' : """
                                                 Mesh_Descriptor_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 DescriptorUI TEXT,
                                                 Mesh_Descriptor TEXT"""},
                            {
                             'tableName' : 'tbl_Mesh_Qualifiers',
                             'tableColumns' : """
                                                 Mesh_Qualifier_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 QualifierUI TEXT,
                                                 Mesh_Qualifier TEXT"""},
                            {
                             'tableName' : 'tbl_Publication_Mesh_Terms',
                             'tableColumns' : """
                                                 Publication_Mesh_Term_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Publication_ID INTEGER,
                                                 Mesh_Descriptor_ID INTEGER,
                                                 Major_Descriptor INTEGER,
                                                 Mesh_Qualifier_ID INTEGER,
                                                 Major_Qualifier INTEGER"""},
                            {
                             'tableName' : 'tbl_Investigator_Coauthors_Searched',
                             'tableColumns' : """
                                                 Investigator_Coauthor_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Investigator_ID INTEGER,
                                                 Coauthor_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_PMCID_to_PMCID_Citations',
                             'tableColumns' : """
                                                 PMCID_to_PMCID_Citation_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 PMCID_Cited_Publication_ID INTEGER,
                                                 PMCID_Cited_by_Publication_id INTEGER"""},
                            {
                             'tableName' : 'tbl_PubMed_ID_to_PubMed_ID_Citations',
                             'tableColumns' : """
                                                 PubMed_ID_to_PubMed_ID_Citation_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 PubMed_ID_Cited_Publication_ID INTEGER,
                                                 PubMed_ID_Cited_by_Publication_id INTEGER"""},
                            {
                             'tableName' : 'tbl_PubMed_ID_to_PubMed_ID_Neighbors',
                             'tableColumns' : """
                                                 PubMed_ID_to_PubMed_ID_Neighbor_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                                 Publication_ID INTEGER,
                                                 Neighbor_PubMed_ID TEXT,
                                                 Neighbor_Score INTEGER"""}]
        
    def delete_publications_tables(self):
        
        for database_table_item in self.database_table_parts:
            
            try:
                self.database_cursor.execute("DROP TABLE " + database_table_item['tableName'] + ";")
                self.database_connection.commit()
                print('Dropped table: ' + database_table_item['tableName'].strip())
            except sqlite3.OperationalError as oer:
                if oer.args[0][:14] == 'no such table:':
                    print('Drop table skipped: ' + database_table_item['tableName'].strip())
                else:
                    raise oer
        
        return True
        
    def create_publications_tables(self):
            
        try:
            self.database_cursor.execute("DROP TABLE tbl_Investigator_Coauthors_Searched;")
            self.database_connection.commit()
            print('Dropped table: tbl_Investigator_Coauthors_Searched')
        except sqlite3.OperationalError as oer:
            if oer.args[0][:14] == 'no such table:':
                print('Drop table skipped: tbl_Investigator_Coauthors_Searched')
            else:
                raise oer
        
        for database_table_item in self.database_table_parts:
            
            try:
                self.database_cursor.execute("CREATE TABLE " + database_table_item['tableName'] + "(" + database_table_item['tableColumns'] + ");")
                self.database_connection.commit()
                print('Created table: ' + database_table_item['tableName'].strip())
            except sqlite3.ProgrammingError as per:
                if per.args[0] == '42S01':
                    print('Create table skipped: ' + database_table_item['tableName'].strip())
                else:
                    raise per
        
        return True
   
    def lookup_publication_id_by_pubmed_id(self, pubmed_id_in):
        
        db_pstatement = None
        db_result = None
        return_id = None
        
        query_pubmed_id = None
        try:
            query_pubmed_id = pubmed_id_in.strip()
        except TypeError:
            pass
        
        db_result = self.database_cursor.execute("SELECT Publication_ID FROM tbl_PubMed_Publications WHERE PubMed_ID = ?;", (str(query_pubmed_id),)).fetchone()
        if db_result:
            return_id = int(db_result['Publication_ID'])
        
        return return_id
    
    def insert_update_publication(
                                  self,
                                  pubmed_id_in,
                                  journal_title_in,
                                  pmcid_in,
                                  medline_date_in,
                                  authors_in,
                                  article_title_in,
                                  affiliation_in,
                                  volume_in,
                                  issue_in,
                                  pagination_in,
                                  article_year_in,
                                  article_month_in,
                                  author_list_complete_in,
                                  grant_list_complete_in,
                                  pubmed_xml_in):
        
        return_publication_id = None
        
        return_publication_id = self.lookup_publication_id_by_pubmed_id(pubmed_id_in)
                
        if return_publication_id:
            
            sql_query = None
            sql_query_fields = """
                                  Journal = ?,
                                  Medline_Date = ?,
                                  Authors = ?,
                                  Title = ?,
                                  Affiliation = ?,
                                  PubMed_XML = ?,
                                  PubMed_XML_Date = datetime('now')"""
            
            db_parameters = []
            
            db_parameters.append(journal_title_in)
            db_parameters.append(medline_date_in)
            db_parameters.append(authors_in)
            db_parameters.append(article_title_in)
            db_parameters.append(affiliation_in)
            db_parameters.append(pubmed_xml_in)
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID = ?"
                db_parameters.append(pmcid_in)
            
            if volume_in != None:
                sql_query_fields += ", Volume = ?"
                db_parameters.append(volume_in)
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number = ?"
                db_parameters.append(issue_in)
                
            if pagination_in != None:
                sql_query_fields += ", Pages = ?"
                db_parameters.append(pagination_in)
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year = ?"
                db_parameters.append(article_year_in)
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month = ?"
                db_parameters.append(article_month_in)
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete = ?"
                db_parameters.append(author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete = ?"
                db_parameters.append(grant_list_complete_in)
                    
            sql_query = "UPDATE tbl_PubMed_Publications SET " + sql_query_fields + " WHERE Publication_ID = ?;"
                    
            db_parameters.append(return_publication_id)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Updated Publication ID ' + str(return_publication_id))
            
        else:
            
            db_result = None
            sql_query = None
            
            sql_query_fields = """
                                  PubMed_ID,
                                  Journal,
                                  Medline_Date,
                                  Authors,
                                  Title,
                                  Affiliation,
                                  PubMed_XML,
                                  PubMed_XML_Date"""
            
            sql_query_values = "?, ?, ?, ?, ?, ?, ?, datetime('now')"
                                                                         
            db_parameters = []

            db_parameters.append(pubmed_id_in)
            db_parameters.append(journal_title_in)
            db_parameters.append(medline_date_in)
            db_parameters.append(authors_in)
            db_parameters.append(article_title_in)
            db_parameters.append(affiliation_in)
            
            # FIGURE OUT WHAT IS WRONG HERE!!!!
            try:
                db_parameters.append(pubmed_xml_in.encode('utf-8'))
            except AttributeError:
                db_parameters.append(None)
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID"
                sql_query_values += ", ?"
                db_parameters.append(pmcid_in)
            
            if volume_in != None:
                sql_query_fields += ", Volume"
                sql_query_values += ", ?"
                db_parameters.append(volume_in)
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number"
                sql_query_values += ", ?"
                db_parameters.append(issue_in)
                
            if pagination_in != None:
                sql_query_fields += ", Pages"
                sql_query_values += ", ?"
                db_parameters.append(pagination_in)
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year"
                sql_query_values += ", ?"
                db_parameters.append(article_year_in)
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month"
                sql_query_values += ", ?"
                db_parameters.append(article_month_in)
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete"
                sql_query_values += ", ?"
                db_parameters.append(author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete"
                sql_query_values += ", ?"
                db_parameters.append(grant_list_complete_in)
                
            sql_query = (
                         "INSERT INTO tbl_PubMed_Publications (" +
                         sql_query_fields +
                         ") VALUES (" +
                         sql_query_values +
                         ");")
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            db_result = self.database_cursor.execute("SELECT last_insert_rowid() AS Publication_ID;").fetchone()
            
            if db_result:
                return_publication_id = int(db_result['Publication_ID'])
                
            print('Inserted Publication ID ' + str(return_publication_id))
        
        return return_publication_id
    
    def get_coauthor_id(self, last_name_in, first_name_in, initials_in, affiliation_in):
        
        return_coauthor_id = None

        db_parameters = []
        db_result = None
        
        sql_query = """
                       SELECT Coauthor_ID
                       FROM tbl_Coauthors
                       WHERE """
        
        if last_name_in:
            sql_query += " upper(trim(?)) = upper(trim(LastName)) "
            db_parameters.append(last_name_in)
        else:
            sql_query += " LastName IS NULL "
        
        if first_name_in:
            sql_query += " AND upper(trim(?)) = upper(trim(FirstName)) "
            db_parameters.append(first_name_in)
        else:
            sql_query += " AND FirstName IS NULL "
        
        if initials_in:
            sql_query += " AND upper(trim(?)) = upper(trim(Initials)) "
            db_parameters.append(initials_in)
        else:
            sql_query += " AND Initials IS NULL "
        
        if affiliation_in:
            sql_query += " AND upper(trim(?)) = upper(trim(Affiliation)) "
            db_parameters.append(affiliation_in)
        else:
            sql_query += " AND Affiliation IS NULL "
            
        sql_query += ";"
            
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
            
        if db_result:
            return_coauthor_id = int(db_result['Coauthor_ID'])
        
        if not return_coauthor_id:

            db_parameters = []
            
            db_parameters.append(last_name_in)
            db_parameters.append(first_name_in)
            db_parameters.append(initials_in)
            db_parameters.append(affiliation_in)
            
            self.database_cursor.execute("INSERT INTO tbl_Coauthors (LastName, FirstName, Initials, Affiliation) VALUES (?, ?, ?, ?);", db_parameters)
            self.database_connection.commit()
            
            db_result = self.database_cursor.execute("SELECT last_insert_rowid() AS Coauthor_ID;").fetchone()
            
            if db_result:
                return_coauthor_id = int(db_result['Coauthor_ID'])
        
        return return_coauthor_id
    
    def join_publication_coauthor(self, publication_id_in, coauthor_id_in):
        
        db_parameters = []
        db_result = None
        
        check_publication_coauthor = 0
        
        sql_query = """
                       SELECT count(*) AS Publication_Coauthor_Count
                       FROM tbl_Publication_Coauthors
                       WHERE
                           Publication_ID = ? AND
                           Coauthor_ID = ?
                       ;"""
        
        db_parameters.append(publication_id_in)
        db_parameters.append(coauthor_id_in)
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            check_publication_coauthor = int(db_result['Publication_Coauthor_Count'])
        
        if check_publication_coauthor == 0:
            
            sql_query = """
                           INSERT INTO tbl_Publication_Coauthors (
                               Publication_ID, Coauthor_ID
                           ) VALUES (
                               ?, ?
                           );"""

            db_parameters = []
            
            db_parameters.append(publication_id_in)
            db_parameters.append(coauthor_id_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
        
    def get_mesh_descriptor_id (self, mesh_descriptor_in):

        db_result = None
        mesh_descriptor_id = None
        
        sql_query = """
                       SELECT Mesh_Descriptor_ID
                       FROM tbl_Mesh_Descriptors
                       WHERE upper(trim(Mesh_Descriptor)) = upper(trim(?))
                       ;"""

        db_result = self.database_connection.execute(sql_query, (mesh_descriptor_in,)).fetchone()
        
        if db_result:
            mesh_descriptor_id = int(db_result['Mesh_Descriptor_ID'])
        
        if mesh_descriptor_id == None:

            self.database_cursor.execute("INSERT INTO tbl_Mesh_Descriptors (Mesh_Descriptor) VALUES (?);", (mesh_descriptor_in,))
            self.database_connection.commit()
            
            db_result = self.database_cursor.execute("SELECT last_insert_rowid() AS Mesh_Descriptor_ID;").fetchone()
            
            if db_result:
                mesh_descriptor_id = int(db_result['Mesh_Descriptor_ID'])
            
        return mesh_descriptor_id
        
    def get_mesh_qualifier_id (self, mesh_qualifier_in):
        
        db_result = None
        mesh_qualifier_id = None
        
        sql_query = """
                       SELECT Mesh_Qualifier_ID
                       FROM tbl_Mesh_Qualifiers
                       WHERE upper(trim(Mesh_Qualifier)) = upper(trim(?))
                       ;"""
        
        db_result = self.database_cursor.execute(sql_query, (mesh_qualifier_in,)).fetchone()
        
        if db_result:
            mesh_qualifier_id = int(db_result['Mesh_Qualifier_ID'])
        
        if mesh_qualifier_id == None:
            
            self.database_cursor.execute("INSERT INTO tbl_Mesh_Qualifiers (Mesh_Qualifier) VALUES (?);", (mesh_qualifier_in,))
            self.database_connection.commit()
            
            db_result = self.database_cursor.execute("SELECT last_insert_rowid() AS Mesh_Qualifier_ID;").fetchone()
            
            if db_result:
                mesh_qualifier_id = int(db_result['Mesh_Qualifier_ID'])
            
        return mesh_qualifier_id
                    
    def join_publication_mesh_terms(
                                    self,
                                    publication_id_in,
                                    descriptor_id_in,
                                    descriptor_major_topic_in,
                                    qualifier_id_in,
                                    qualifier_major_topic_in):
        
        db_result = None
        publication_mesh_term_id = None
        
        sql_query = """
                       SELECT Publication_Mesh_Term_ID
                       FROM tbl_Publication_Mesh_Terms
                       WHERE
                           Publication_ID = ? AND
                           Mesh_Descriptor_ID = ? """
                           
        if qualifier_id_in != None:
            sql_query += " AND Mesh_Qualifier_ID = ? "
        else:
            sql_query += " AND Mesh_Qualifier_ID IS NULL "
        
        sql_query += ";"

        db_parameters = []
        
        db_parameters.append(publication_id_in)
        db_parameters.append(descriptor_id_in)
        
        if qualifier_id_in != None:
            db_parameters.append(qualifier_id_in)
            
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            publication_mesh_term_id = int(db_result['Publication_Mesh_Term_ID'])
            
        if publication_mesh_term_id == None:
            
            sql_query_fields = "Publication_ID, Mesh_Descriptor_ID, Major_Descriptor"
            sql_query_values = "?, ?, ?"
            
            if qualifier_id_in != None:
                sql_query_fields += ", Mesh_Qualifier_ID"
                sql_query_values += ", ?"
                
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier"
                sql_query_values += ", ?"
            
            db_parameters = []

            db_parameters.append(publication_id_in)
            db_parameters.append(descriptor_id_in)
            db_parameters.append(descriptor_major_topic_in)
            
            if qualifier_id_in != None:
                db_parameters.append(qualifier_id_in)
                
            if qualifier_major_topic_in != None:
                db_parameters.append(qualifier_major_topic_in)
            
            self.database_cursor.execute("INSERT INTO tbl_Publication_Mesh_Terms (" + sql_query_fields + ") VALUES (" + sql_query_values + ");", db_parameters)
            self.database_connection.commit()
            
        else:
            
            sql_query_fields = "Major_Descriptor = ?"
            
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier = ?"

            db_parameters = []
            
            db_parameters.append(descriptor_major_topic_in)
            
            if qualifier_major_topic_in != None:
                db_parameters.append(qualifier_major_topic_in)
            
            db_parameters.append(publication_mesh_term_id)
            
            self.database_cursor.execute("UPDATE tbl_Publication_Mesh_Terms SET " + sql_query_fields + " WHERE Publication_Mesh_Term_ID = ?;", db_parameters)
            self.database_connection.commit()
            
    def get_grant_id(
                     self,
                     grant_number_in,
                     phs_activity_code_in,
                     phs_organization_in,
                     phs_grant_number_in,
                     acronym_in,
                     agency_in,
                     country_in,
                     ctsa_grant_number_id_in):
        
        db_result = None
        return_grant_id = None
        
        sql_query = "SELECT Grant_ID FROM tbl_Grants WHERE Grant_Number = ? "
                           
        if phs_activity_code_in != None:
            sql_query += " AND PHS_Activity_Code = ? "
        else:
            sql_query += " AND PHS_Activity_Code IS NULL "
                           
        if phs_organization_in != None:
            sql_query += " AND PHS_Organization = ? "
        else:
            sql_query += " AND PHS_Organization IS NULL "
                           
        if phs_grant_number_in != None:
            sql_query += " AND PHS_Six_Digit_Grant_Number = ? "
        else:
            sql_query += " AND PHS_Six_Digit_Grant_Number IS NULL "
                           
        if agency_in != None:
            sql_query += " AND Agency = ? "
        else:
            sql_query += " AND Agency IS NULL "
                           
        if country_in != None:
            sql_query += " AND Country = ? "
        else:
            sql_query += " AND Country IS NULL "
        
        sql_query += ";"

        db_parameters = []
        
        db_parameters.append(grant_number_in)
        
        if phs_activity_code_in != None:
            db_parameters.append(phs_activity_code_in)
            
        if phs_organization_in != None:
            db_parameters.append(phs_organization_in)
            
        if phs_grant_number_in != None:
            db_parameters.append(phs_grant_number_in)
            
        if agency_in != None:
            db_parameters.append(agency_in)
            
        if country_in != None:
            db_parameters.append(country_in)
            
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            return_grant_id = int(db_result['Grant_ID'])
            
        if return_grant_id == None:
            
            sql_query_fields = "Grant_Number, Acronym, Agency, Country"
            sql_query_values = "?, ?, ?, ?"

            db_parameters = []
            
            db_parameters.append(grant_number_in)
            db_parameters.append(acronym_in)
            db_parameters.append(agency_in)
            db_parameters.append(country_in)
            
            if phs_activity_code_in != None:
                sql_query_fields += ", PHS_Activity_Code"
                sql_query_values += ", ?"
                db_parameters.append(phs_activity_code_in)
                
            if phs_organization_in != None:
                sql_query_fields += ", PHS_Organization"
                sql_query_values += ", ?"
                db_parameters.append(phs_organization_in)
                
            if phs_grant_number_in != None:
                sql_query_fields += ", PHS_Six_Digit_Grant_Number"
                sql_query_values += ", ?"
                db_parameters.append(phs_grant_number_in)
            
            if ctsa_grant_number_id_in != None:
                sql_query_fields += ", CTSA_Grant_Number_ID"
                sql_query_values += ", ?"
                db_parameters.append(int(ctsa_grant_number_id_in))
            
            self.database_cursor.execute("INSERT INTO tbl_Grants (" + sql_query_fields + ") VALUES (" + sql_query_values + ");", db_parameters)
            self.database_connection.commit()
            
            db_result = self.database_cursor.execute("SELECT last_insert_rowid() AS Grant_ID;").fetchone()
        
            if db_result:
                return_grant_id = int(db_result['Grant_ID'])
            
        else:
            
            sql_query_fields = "Acronym = ?, Agency = ?, Country = ?"

            db_parameters = []
            
            db_parameters.append(acronym_in)
            db_parameters.append(agency_in)
            db_parameters.append(country_in)
            
            if ctsa_grant_number_id_in != None:
                sql_query_fields += ", CTSA_Grant_Number_ID = ?"
                db_parameters.append(ctsa_grant_number_id_in)
            
            db_parameters.append(return_grant_id)
            
            self.database_cursor.execute("UPDATE tbl_Grants SET " + sql_query_fields + " WHERE Grant_ID = ?;", db_parameters)
            self.database_connection.commit()
            
        return return_grant_id
    
    def get_ctsa_grant_id(self, phs_organization_in, phs_grant_number_in):
        
        row = None
        return_grant_id = None
        
        sql_query = """
                       SELECT DISTINCT
                           CTSA_Grant_Number_ID
                       FROM tbl_CTSA_Grant_Numbers
                       WHERE
                           PHS_Organization = ? AND
                           PHS_Six_Digit_Grant_Number = ?
                       ;"""
                       
        row = self.database_cursor.execute(sql_query, (phs_organization_in, phs_grant_number_in.zfill(6))).fetchone()
        
        if row:
            return_grant_id = int(row['CTSA_Grant_Number_ID'])
        
        return return_grant_id
    
    def join_publication_grant(self, publication_id_in, grant_id_in):
        
        db_result = None
        publication_grants_cited_id = None
        
        sql_query = """
                       SELECT
                           Publication_Grants_Cited_ID
                       FROM tbl_Publication_Grants_Cited
                       WHERE
                           Publication_ID = ? AND
                           Grant_ID = ?
                       ;"""

        db_parameters = []
        
        db_parameters.append(publication_id_in)
        db_parameters.append(grant_id_in)
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
    
        if db_result:
            publication_grants_cited_id = int(db_result['Publication_Grants_Cited_ID'])
            
        if publication_grants_cited_id == None:
            
            sql_query = """
                           INSERT INTO tbl_Publication_Grants_Cited (
                               Publication_ID, Grant_ID
                           ) VALUES (
                               ?, ?
                           );"""

            db_parameters = []
        
            db_parameters.append(publication_id_in)
            db_parameters.append(grant_id_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
    
    def join_investigator_publication(self, investigator_id_in, publication_id_in):

        db_result = None
        check_investigator_publication = 0
        
        sql_query = """
                       SELECT count(*) AS Investigator_Publication_Count
                       FROM tbl_Investigator_Publications
                       WHERE
                           Investigator_ID = ? AND
                           Publication_ID = ?
                       ;"""
        
        db_parameters = []
        
        db_parameters.append(investigator_id_in)
        db_parameters.append(publication_id_in)
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            check_investigator_publication = int(db_result['Investigator_Publication_Count'])
        
        if check_investigator_publication == 0:
            
            sql_query = """
                           INSERT INTO tbl_Investigator_Publications (
                               Investigator_ID, Publication_ID
                           ) VALUES (
                               ?, ?
                           );"""

            db_parameters = []
            
            db_parameters.append(investigator_id_in)
            db_parameters.append(publication_id_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(publication_id_in) + ' to Investigator ID ' + str(investigator_id_in))
    
    def join_investigator_coauthor_searched(self, investigator_id_in, coauthor_id_in):
        
        db_result = None
        check_investigator_coauthor = 0
        
        sql_query = """
                       SELECT count(*) AS Investigator_Coauthor_Count
                       FROM tbl_Investigator_Coauthors_Searched
                       WHERE
                           Investigator_ID = ? AND
                           Coauthor_ID = ?
                       ;"""

        db_parameters = []
        
        db_parameters.append(investigator_id_in)
        db_parameters.append(coauthor_id_in)
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            check_investigator_coauthor = int(db_result['Investigator_Coauthor_Count'])
        
        if check_investigator_coauthor == 0:
            
            sql_query = """
                           INSERT INTO tbl_Investigator_Coauthors_Searched (
                               Investigator_ID, Coauthor_ID
                           ) VALUES (
                               ?, ?
                           );"""

            db_parameters = []
        
            db_parameters.append(investigator_id_in)
            db_parameters.append(coauthor_id_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
    
    def join_pmcid_cited_by_pmcid(self, pmcid_cited_publication_id_in, pmcid_cited_by_publication_id_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT count(*) AS PMCID_to_PMCID_Citation_Count
                       FROM tbl_PMCID_to_PMCID_Citations
                       WHERE
                           PMCID_Cited_Publication_ID = ? AND
                           PMCID_Cited_by_Publication_ID = ?
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pmcid_cited_publication_id_in))
        db_parameters.append(int(pmcid_cited_by_publication_id_in))
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result['PMCID_to_PMCID_Citation_Count'])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PMCID_to_PMCID_Citations (
                               PMCID_Cited_Publication_ID, PMCID_Cited_by_Publication_ID
                           ) VALUES (
                               ?, ?
                           );"""
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pmcid_cited_publication_id_in) + ' cited by Publication ID ' + str(pmcid_cited_by_publication_id_in) + ' in PubMed Central')
    
    def join_pubmed_id_cited_by_pubmed_id(self, pubmed_id_cited_publication_id_in, pubmed_id_cited_by_publication_id_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT count(*) AS PubMed_ID_to_PubMed_ID_Citation_Count
                       FROM tbl_PubMed_ID_to_PubMed_ID_Citations
                       WHERE
                           PubMed_ID_Cited_Publication_ID = ? AND
                           PubMed_ID_Cited_by_Publication_ID = ?
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pubmed_id_cited_publication_id_in))
        db_parameters.append(int(pubmed_id_cited_by_publication_id_in))
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result['PubMed_ID_to_PubMed_ID_Citation_Count'])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PubMed_ID_to_PubMed_ID_Citations (
                               PubMed_ID_Cited_Publication_ID, PubMed_ID_Cited_by_Publication_ID
                           ) VALUES (
                               ?, ?
                           );"""
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pubmed_id_cited_publication_id_in) + ' cited by Publication ID ' + str(pubmed_id_cited_by_publication_id_in) + ' in PubMed')
    
    def join_pubmed_id_neighbor_pubmed_id(self, pubmed_id_publication_id_in, pubmed_id_neighbor_in, neighbor_score_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT COUNT(*) AS PubMed_ID_to_PubMed_ID_Neighbor_Count
                       FROM tbl_PubMed_ID_to_PubMed_ID_Neighbors
                       WHERE
                           Publication_ID = ? AND
                           Neighbor_PubMed_ID = ?
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pubmed_id_publication_id_in))
        db_parameters.append(pubmed_id_neighbor_in)
        
        db_result = self.database_cursor.execute(sql_query, db_parameters).fetchone()
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result['PubMed_ID_to_PubMed_ID_Neighbor_Count'])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PubMed_ID_to_PubMed_ID_Neighbors (
                               Publication_ID, Neighbor_PubMed_ID, Neighbor_Score
                           ) VALUES (
                               ?, ?, ?
                           );"""
            
            db_parameters.append(neighbor_score_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pubmed_id_publication_id_in) + ' neighbor to PubMed ID ' + str(pubmed_id_neighbor_in))
            
        else:

            db_parameters = []
            
            sql_query = """
                           UPDATE tbl_PubMed_ID_to_PubMed_ID_Neighbors
                           SET
                               Neighbor_Score = ?
                           WHERE
                               Publication_ID = ? AND
                               Neighbor_PubMed_ID = ?
                           ;"""
            
            db_parameters.append(neighbor_score_in)
            db_parameters.append(int(pubmed_id_publication_id_in))
            db_parameters.append(pubmed_id_neighbor_in)
            
            self.database_cursor.execute(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Updated Publication ID ' + str(pubmed_id_publication_id_in) + ' neighbor to PubMed ID ' + str(pubmed_id_neighbor_in))
        
    def get_pmcid_from_pubmed_id(self, pubmed_id_in):
        
        db_result = None
        pmcid_result = None
        
        sql_query = """
                       SELECT PMCID
                       FROM tbl_PubMed_Publications
                       WHERE trim(PubMed_ID) = trim(?)
                       ;"""
        
        db_result = self.database_cursor.execute(sql_query, (pubmed_id_in, )).fetchone()
        
        if db_result:
            
            try:
                pmcid_result = db_result['PMCID'].strip()
            except AttributeError:
                pass
            
        return pmcid_result
