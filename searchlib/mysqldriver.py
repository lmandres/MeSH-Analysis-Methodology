import mysql.connector

class PubMedMySQLDatabaseController:
    '''
    classdocs
    '''

    database_host = None
    database_user = None
    database_passwd = None
    database_db = None

    database_connection = None
    database_cursor = None
    
    def open_database(self, host = None, user = None, password = None, database = None):

        self.database_host = host
        self.database_user = user
        self.database_passwd = password
        self.database_db = database
        
        try:

            self.database_connection = mysql.connector.connect(
                                                		host = self.database_host,
                                                		user = self.database_user,
                                                		password = self.database_passwd,
                                                		database = self.database_db
                                                		)
            self.database_cursor = self.database_connection.cursor()

            return True

        except TypeError:

            self.database_connection = None
            return False
        
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
    
    def run_sql_query(self, sql_query_in, sql_params_in = None):

        def run_sql_query_local(local_sql_query_in, local_sql_params_in = None):
            if sql_params_in:
                return self.database_cursor.execute(local_sql_query_in, local_sql_params_in)
            else:
                return self.database_cursor.execute(local_sql_query_in)

        try:
                return run_sql_query_local(sql_query_in, sql_params_in)
        except mysql.connector.errors.InterfaceError as ie:
            
            if ie.errno == 2013:
                
                self.close_database()
                self.open_database(
                    host = self.database_host,
                    user = self.database_user,
                    password = self.database_passwd,
                    database = self.database_db)

                return run_sql_query_local(sql_query_in, sql_params_in)
                
            else:
                raise ie
            
        except mysql.connector.errors.OperationalError as oe:
            
            if oe.errno == 2055:
                
                self.close_database()
                self.open_database(
                    host = self.database_host,
                    user = self.database_user,
                    password = self.database_passwd,
                    database = self.database_db)

                return run_sql_query_local(sql_query_in, sql_params_in)
            
            else:
                raise oe

class PubMedMySQLDatabaseSearch(PubMedMySQLDatabaseController):
    
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
                                                                              WHEN sq_1.FAMILY_NAME IS NOT NULL THEN Trim(sq_1.FAMILY_NAME)
                                                                              ELSE ''
                                                                          END AS Search_Person_Last_Name,
                                                                          CASE
                                                                              WHEN sq_1.FIRST_GIVEN_NAME IS NOT NULL THEN Trim(sq_1.FIRST_GIVEN_NAME)
                                                                              ELSE ''
                                                                          END AS Search_Person_First_Name,
                                                                          CASE
                                                                              WHEN sq_1.SECOND_GIVEN_NAME IS NOT NULL THEN Left(sq_1.SECOND_GIVEN_NAME, 1)
                                                                              ELSE ''
                                                                          END AS Search_Person_Middle_Name,
                                                                          
                                                                          sq_2.PERSON_ID AS Co_Investigator_ID,
                                                                          CASE
                                                                              WHEN sq_2.FAMILY_NAME IS NOT NULL THEN Trim(sq_2.FAMILY_NAME)
                                                                              ELSE ''
                                                                          END AS Co_Investigator_Last_Name,
                                                                          CASE
                                                                              WHEN sq_2.FIRST_GIVEN_NAME IS NOT NULL THEN Trim(sq_2.FIRST_GIVEN_NAME)
                                                                              ELSE ''
                                                                          END AS Co_Investigator_First_Name,
                                                                          CASE
                                                                              WHEN sq_2.SECOND_GIVEN_NAME IS NOT NULL THEN Left(sq_2.SECOND_GIVEN_NAME, 1)
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
                                                                                  WHEN sq_1_1.FAMILY_NAME IS NOT NULL THEN Trim(sq_1_1.FAMILY_NAME)
                                                                                  ELSE ''
                                                                              END AS Search_Person_Last_Name,
                                                                              CASE
                                                                                  WHEN sq_1_1.FIRST_GIVEN_NAME IS NOT NULL THEN Trim(sq_1_1.FIRST_GIVEN_NAME)
                                                                                  ELSE ''
                                                                              END AS Search_Person_First_Name,
                                                                              CASE
                                                                                  WHEN sq_1_1.SECOND_GIVEN_NAME IS NOT NULL THEN Left(sq_1_1.SECOND_GIVEN_NAME, 1)
                                                                                  ELSE ''
                                                                              END AS Search_Person_Middle_Name,
                                                                             Trim(aol_1_1.ACTSI_ORGANIZATION_NAME) AS ACTSI_Organization""",
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
                                                                          WHEN sq_1.FAMILY_NAME IS NOT NULL THEN Trim(sq_1.FAMILY_NAME)
                                                                          ELSE ''
                                                                      END AS Search_Person_Last_Name,
                                                                      CASE
                                                                          WHEN sq_1.FIRST_GIVEN_NAME IS NOT NULL THEN Trim(sq_1.FIRST_GIVEN_NAME)
                                                                          ELSE ''
                                                                      END AS Search_Person_First_Name,
                                                                      CASE
                                                                          WHEN sq_1.SECOND_GIVEN_NAME IS NOT NULL THEN Left(sq_1.SECOND_GIVEN_NAME, 1)
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
										CASE
											WHEN fol_1_1.FUNDING_ORG_ABBREVIATION IS NOT NULL THEN Trim(fol_1_1.FUNDING_ORG_ABBREVIATION)
											ELSE ''
										END AS PHS_Organization,
										CASE
											WHEN rg_1_1.GRANT_NUMBER IS NOT NULL THEN Lpad(rg_1_1.GRANT_NUMBER, 6, '000000')
											ELSE ''
										END AS PHS_Six_Digit_Grant_Number
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
                                                                                          WHEN pr_1_1.FAMILY_NAME IS NOT NULL THEN Trim(pr_1_1.FAMILY_NAME)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_Last_Name,
                                                                                      CASE
                                                                                          WHEN pr_1_1.FIRST_GIVEN_NAME IS NOT NULL THEN Trim(pr_1_1.FIRST_GIVEN_NAME)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_First_Name,
                                                                                      CASE
                                                                                          WHEN pr_1_1.SECOND_GIVEN_NAME IS NOT NULL THEN Left(pr_1_1.SECOND_GIVEN_NAME, 1)
                                                                                          ELSE ''
                                                                                      END AS Search_Person_Middle_Name,
                                                                                      
                                                                                   ca_1_1.Coauthor_ID,
                                                                                   CASE
                                                                                       WHEN ca_1_1.LastName IS NOT NULL THEN Trim(ca_1_1.LastName)
                                                                                       ELSE ''
                                                                                   END AS Coauthor_Last_Name,
                                                                                   CASE
                                                                                       WHEN ca_1_1.FirstName IS NOT NULL THEN Trim(ca_1_1.FirstName)
                                                                                       ELSE ''
                                                                                   END AS Coauthor_First_Name,
                                                                                   CASE
                                                                                       WHEN ca_1_1.Initials IS NOT NULL THEN Trim(ca_1_1.Initials)
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
                                                                                WHERE (Trim(ca_1_1.LastName) NOT LIKE '%' + trim(pr_1_1.FAMILY_NAME) + '%' AND
                                                                                        Left(Trim(ca_1_1.FirstName), 1) <> Left(Trim(pr_1_1.FIRST_GIVEN_NAME), 1)) OR
                                                                                        (Trim(pr_1_1.FAMILY_NAME) NOT LIKE '%' + Trim(ca_1_1.LastName) + '%' AND
                                                                                        Left(Trim(pr_1_1.FIRST_GIVEN_NAME), 1) <> Left(Trim(ca_1_1.FirstName), 1))
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
    search_terms_resultset_index = 0

    current_person_id = None
    current_coinvestigator_id = None
    current_coauthor_id = None
    current_ctsa_grant_id = None

    current_running_flag = None
    
    database_search_filename = None

    def get_search_recordset(self, search_strategy_in):
        
        self.run_sql_query(
                                            "SELECT " +
                                            self.search_method_query_parts[search_strategy_in]['selectFields'] + " " +
                                            self.search_method_query_parts[search_strategy_in]['fromClause'] + " " +
                                            self.search_method_query_parts[search_strategy_in]['orderByClause'] + ";")
        
        return self.database_cursor.fetchall()
    
    def get_search_recordcount(self, search_strategy_in):
        
        return_recordcount = 0
        
        count_recordset = self.run_sql_query(
                                             "SELECT COUNT(*) AS Search_Term_Count " +
                                             self.search_method_query_parts[search_strategy_in]['fromClause'] + ";")
        
        row = count_recordset.fetchone()
        return_recordcount = int(row[0])
        count_recordset.close()
        
        return return_recordcount
        
    def get_search_term(self):
        
        return_search_term = None
        
        if self.search_terms_resultset == None:
            
            if self.current_running_flag == self.CTSA_GRANT:
            
                try:
                    self.run_sql_query("DROP TABLE tbl_CTSA_Grant_Searches;")
                    self.database_connection.commit()
                    print('Dropped table: tbl_CTSA_Grant_Searches')
                except mysql.connector.ProgrammingError as per:
                    if per.errno == 1051:
                        print('Drop table skipped: tbl_CTSA_Grant_Searches')
                    else:
                        raise per
                    
                try:
                    self.run_sql_query("""
                                                    CREATE TABLE tbl_CTSA_Grant_Searches (
                                                        CTSA_Grant_Search_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                        CTSA_Grant_Number_ID MEDIUMINT,
                                                        CTSA_Grant_Number VARCHAR(255),
                                                    PRIMARY KEY (CTSA_Grant_Search_ID)
                                                    );""")
                    self.database_connection.commit()
                    print('Created table: tbl_CTSA_Grant_Searches')
                except mysql.connector.ProgrammingError as per:
                    if per.errno == 1050:
                        print('Create table skipped: tbl_CTSA_Grant_Searches')
                    else:
                        raise per
                               
                sql_insert_query = """
                                        INSERT INTO tbl_CTSA_Grant_Searches (
                                            CTSA_Grant_Number_ID,
                                            CTSA_Grant_Number
                                        ) VALUES (
                                            %s, %s
                                        );"""
                
                sql_select_query = """
                                        SELECT DISTINCT gn_1.CTSA_Grant_Number_ID, gn_1.PHS_Activity_Code, gn_1.PHS_Organization, gn_1.PHS_Six_Digit_Grant_Number
                                        FROM tbl_CTSA_Grant_Numbers AS gn_1
                                        ORDER BY gn_1.PHS_Organization, gn_1.PHS_Six_Digit_Grant_Number
                                        ;"""
                
                self.run_sql_query(sql_select_query)
                for row in self.database_cursor.fetchall():
                    
                    phs_grant_number = row[3].lstrip('0').strip()
                    
                    for number_index in range(0, 6-len(phs_grant_number)+1, 1):
                        
                        phs_edited_grant_number = row[2].strip() + ("0" * number_index) + phs_grant_number
                        self.run_sql_query(sql_insert_query, (int(row[0]), phs_edited_grant_number))
                        self.database_connection.commit()
                        
                        print('Inserted Search Grant Number: ' + phs_edited_grant_number)
                        
                        phs_edited_grant_number = row[1].strip() + row[2].strip() + ("0" * number_index) + phs_grant_number
                        self.run_sql_query(sql_insert_query, (int(row[0]), phs_edited_grant_number))
                        self.database_connection.commit()
                        
                        print('Inserted Search Grant Number: ' + phs_edited_grant_number)
        
            self.search_terms_resultset = self.get_search_recordset(self.current_running_flag)
            self.search_terms_resultset_index = 0
            
        row = None
        try:
            row = self.search_terms_resultset[self.search_terms_resultset_index]
        except IndexError:
            pass
        
        if row:
                        
            running_flag_case = self.current_running_flag                    
            if running_flag_case == self.PERSON_BY_PERSON:
                    
                search_person_term = None
                co_investigator_term = None
                    
                self.current_person_id = int(row[0])
                self.current_coinvestigator_id = int(row[4])
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      row[1].strip() + ', ' +
                                      row[2].strip() + ' ' +
                                      row[3].strip())
                    
                co_investigator_term = (
                                        row[5].strip() + ', ' +
                                        row[6].strip() + ' ' +
                                        row[7].strip())
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + co_investigator_term.strip() + '[au])'
                    
            elif running_flag_case == self.PERSON_ORGANIZATION:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = int(row[0])
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      row[1].strip() + ', ' +
                                      row[2].strip() + ' ' +
                                      row[3].strip())
                    
                organization_term = row[4].strip()
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + organization_term.strip() + '[ad])'
                    
            elif running_flag_case == self.PERSON_GRANT:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = int(row[0])
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      row[1].strip() + ', ' +
                                      row[2].strip() + ' ' +
                                      row[3].strip())
                    
                return_search_term = ('(' + search_person_term.strip() + '[au]) and (' + row[4].strip() + '[gr])')
                    
            elif running_flag_case == self.PERSON_COAUTHOR:
                    
                search_person_term = None
                co_author_term = None
                    
                co_author_last_name = None
                    
                self.current_person_id = int(row[0])
                self.current_coinvestigator_id = None
                self.current_coauthor_id = int(row[4])
                    
                search_person_term = (
                                      row[1].strip() + ', ' +
                                      row[2].strip() + ' ' +
                                      row[3].strip())
                    
                co_author_last_name = row[5].strip()
                    
                co_author_term = (
                                  '(' +
                                  co_author_last_name.strip() + ', ' +
                                  row[6].strip() + '[au]) and (' +
                                  co_author_last_name.strip() + ' ' +
                                  row[7].strip() + '[au])')
                    
                return_search_term = '(' + co_author_term.strip() + ') and (' + search_person_term.strip() + '[au])'
                
            elif running_flag_case == self.CTSA_GRANT:
                    
                self.current_person_id = None
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                
                self.current_ctsa_grant_id = int(row[0])
                
                return_search_term = '(' + row[1].strip() + '[gr])'
                
            self.search_terms_resultset_index += 1
                    
        else:
            self.search_terms_resultset = None
            self.search_terms_resultset_index = 0
            
        return return_search_term

    def get_person_id(self):
        return self.current_person_id

    def get_coinvestigator_id(self):
        return self.current_coinvestigator_id

    def get_coauthor_id(self):
        return self.current_coauthor_id

    def set_running_flag(self, search_strategy_in):
        self.current_running_flag = search_strategy_in

    def get_running_flag(self):
        return self.current_running_flag

class PubMedMySQLDatabaseManager(PubMedMySQLDatabaseController):
    
    database_table_parts = [
                            {
                             'tableName' : 'tbl_PubMed_Publications',
                             'tableColumns' : """
                                                 Publication_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 PubMed_ID VARCHAR(8),
                                                 Journal VARCHAR(255),
                                                 PMCID VARCHAR(11),
                                                 Publish_Month SMALLINT,
                                                 Publish_Year SMALLINT,
                                                 Medline_Date VARCHAR(25),
                                                 Authors VARCHAR(255),
                                                 Title VARCHAR(512),
                                                 Affiliation VARCHAR(255),
                                                 Volume VARCHAR(25),
                                                 Issue_Number VARCHAR(25),
                                                 Pages VARCHAR(25),
                                                 Author_List_Complete BOOLEAN,
                                                 Grant_List_Complete BOOLEAN,
                                                 PubMed_XML TEXT,
                                                 PubMed_XML_Date DATETIME,
						PRIMARY KEY (Publication_ID)"""},
                            {
                             'tableName' : 'tbl_Investigator_Publications',
                             'tableColumns' : """
                                                 Investigator_Publications_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Investigator_ID MEDIUMINT,
                                                 Publication_ID MEDIUMINT,
						PRIMARY KEY (Investigator_Publications_ID)"""},
                            {
                             'tableName' : 'tbl_Grants',
                             'tableColumns' : """
                                                 Grant_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Grant_Number VARCHAR(25),
                                                 PHS_Activity_Code CHAR(3),
                                                 PHS_Organization CHAR(2),
                                                 PHS_Six_Digit_Grant_Number CHAR(6),
                                                 Acronym VARCHAR(25),
                                                 Agency VARCHAR(64),
                                                 Country VARCHAR(25),
                                                 CTSA_Grant_Number_ID MEDIUMINT,
						PRIMARY KEY (Grant_ID)"""},
                            {
                             'tableName' : 'tbl_Publication_Grants_Cited',
                             'tableColumns' : """
                                                 Publication_Grants_Cited_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Publication_ID MEDIUMINT,
                                                 Grant_ID MEDIUMINT,
						PRIMARY KEY (Publication_Grants_Cited_ID)"""},
                            {
                             'tableName' : 'tbl_Coauthors',
                             'tableColumns' : """
                                                 Coauthor_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 LastName VARCHAR(64),
                                                 FirstName VARCHAR(64),
                                                 Initials VARCHAR(5),
                                                 Affiliation VARCHAR(255),
						PRIMARY KEY (Coauthor_ID)"""},
                            {
                             'tableName' : 'tbl_Publication_Coauthors',
                             'tableColumns' : """
                                                 Publication_Coauthor_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Publication_ID MEDIUMINT,
                                                 Coauthor_ID MEDIUMINT,
						PRIMARY KEY (Publication_Coauthor_ID)"""},
                            {
                             'tableName' : 'tbl_Mesh_Descriptors',
                             'tableColumns' : """
                                                 Mesh_Descriptor_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 DescriptorUI VARCHAR(10),
                                                 Mesh_Descriptor VARCHAR(255),
						PRIMARY KEY (Mesh_Descriptor_ID)"""},
                            {
                             'tableName' : 'tbl_Mesh_Qualifiers',
                             'tableColumns' : """
                                                 Mesh_Qualifier_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 QualifierUI VARCHAR(10),
                                                 Mesh_Qualifier VARCHAR(255),
						PRIMARY KEY (Mesh_Qualifier_ID)"""},
                            {
                             'tableName' : 'tbl_Publication_Mesh_Terms',
                             'tableColumns' : """
                                                 Publication_Mesh_Term_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Publication_ID MEDIUMINT,
                                                 Mesh_Descriptor_ID MEDIUMINT,
                                                 Major_Descriptor INTEGER,
                                                 Mesh_Qualifier_ID INTEGER,
                                                 Major_Qualifier INTEGER,
						PRIMARY KEY (Publication_Mesh_Term_ID)"""},
                            {
                             'tableName' : 'tbl_Investigator_Coauthors_Searched',
                             'tableColumns' : """
                                                 Investigator_Coauthor_ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Investigator_ID MEDIUMINT,
                                                 Coauthor_ID MEDIUMINT,
						PRIMARY KEY (Investigator_Coauthor_ID)"""},
                            {
                             'tableName' : 'tbl_PMCID_to_PMCID_Citations',
                             'tableColumns' : """
                                                 PMCID_to_PMCID_Citation_ID INT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 PMCID_Cited_Publication_ID MEDIUMINT,
                                                 PMCID_Cited_by_Publication_id MEDIUMINT,
                        PRIMARY KEY (PMCID_to_PMCID_Citation_ID)"""},
                            {
                             'tableName' : 'tbl_PubMed_ID_to_PubMed_ID_Citations',
                             'tableColumns' : """
                                                 PubMed_ID_to_PubMed_ID_Citation_ID INT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 PubMed_ID_Cited_Publication_ID MEDIUMINT,
                                                 PubMed_ID_Cited_by_Publication_id MEDIUMINT,
                        PRIMARY KEY (PubMed_ID_to_PubMed_ID_Citation_ID)"""},
                            {
                             'tableName' : 'tbl_PubMed_ID_to_PubMed_ID_Neighbors',
                             'tableColumns' : """
                                                 PubMed_ID_to_PubMed_ID_Neighbor_ID INT UNSIGNED NOT NULL AUTO_INCREMENT,
                                                 Publication_ID MEDIUMINT,
                                                 Neighbor_PubMed_ID VARCHAR(8),
                                                 Neighbor_Score INT UNSIGNED,
                        PRIMARY KEY (PubMed_ID_to_PubMed_ID_Neighbor_ID)"""}]
        
    def delete_publications_tables(self):
        
        for database_table_item in self.database_table_parts:
            
            try:
                self.run_sql_query("DROP TABLE " + database_table_item['tableName'] + ";")
                self.database_connection.commit()
                print('Dropped table: ' + database_table_item['tableName'].strip())
            except mysql.connector.ProgrammingError as per:
                if per.errno == 1051:
                    print('Drop table skipped: ' + database_table_item['tableName'].strip())
                else:
                    raise per
        
        return True
        
    def create_publications_tables(self):
            
        try:
            self.run_sql_query("DROP TABLE tbl_Investigator_Coauthors_Searched;")
            self.database_connection.commit()
            print('Dropped table: tbl_Investigator_Coauthors_Searched')
        except mysql.connector.ProgrammingError as per:
            if per.errno == 1051:
                print('Drop table skipped: tbl_Investigator_Coauthors_Searched')
            else:
                raise per
        
        for database_table_item in self.database_table_parts:
            
            try:
                self.run_sql_query("CREATE TABLE " + database_table_item['tableName'] + "(" + database_table_item['tableColumns'] + ");")
                self.database_connection.commit()
                print('Created table: ' + database_table_item['tableName'].strip())
            except mysql.connector.ProgrammingError as per:
                if per.errno == 1050:
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
        
        self.run_sql_query("SELECT Publication_ID FROM tbl_PubMed_Publications WHERE PubMed_ID = %s;", (str(query_pubmed_id),))
        db_result = self.database_cursor.fetchone()
        if db_result:
            return_id = int(db_result[0])
        
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
                                  Journal = %s,
                                  Medline_Date = %s,
                                  Authors = %s,
                                  Title = %s,
                                  Affiliation = %s,
                                  PubMed_XML = %s,
                                  PubMed_XML_Date = Now()"""
            
            db_parameters = []
            
            db_parameters.append(journal_title_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(medline_date_in)
            db_parameters.append(authors_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(article_title_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(affiliation_in)
            db_parameters.append(pubmed_xml_in)
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID = %s"
                db_parameters.append(pmcid_in)
            
            if volume_in != None:
                sql_query_fields += ", Volume = %s"
                db_parameters.append(volume_in)
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number = %s"
                db_parameters.append(issue_in)
                
            if pagination_in != None:
                sql_query_fields += ", Pages = %s"
                db_parameters.append(pagination_in)
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year = %s"
                db_parameters.append(article_year_in)
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month = %s"
                db_parameters.append(article_month_in)
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete = %s"
                db_parameters.append(author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete = %s"
                db_parameters.append(grant_list_complete_in)
                    
            sql_query = "UPDATE tbl_PubMed_Publications SET " + sql_query_fields + " WHERE Publication_ID = %s;"
                    
            db_parameters.append(return_publication_id)
            
            self.run_sql_query(sql_query, db_parameters)
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
            
            sql_query_values = "%s, %s, %s, %s, %s, %s, %s, Now()"
                                                                         
            db_parameters = []
            
            db_parameters.append(pubmed_id_in)
            db_parameters.append(journal_title_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(medline_date_in)
            db_parameters.append(authors_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(article_title_in.encode('utf-8', 'xmlcharrefreplace'))
            db_parameters.append(affiliation_in)
            db_parameters.append(pubmed_xml_in)
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID"
                sql_query_values += ", %s"
                db_parameters.append(pmcid_in)
            
            if volume_in != None:
                sql_query_fields += ", Volume"
                sql_query_values += ", %s"
                db_parameters.append(volume_in)
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number"
                sql_query_values += ", %s"
                db_parameters.append(issue_in)
                
            if pagination_in != None:
                sql_query_fields += ", Pages"
                sql_query_values += ", %s"
                db_parameters.append(pagination_in)
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year"
                sql_query_values += ", %s"
                db_parameters.append(article_year_in)
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month"
                sql_query_values += ", %s"
                db_parameters.append(article_month_in)
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete"
                sql_query_values += ", %s"
                db_parameters.append(author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete"
                sql_query_values += ", %s"
                db_parameters.append(grant_list_complete_in)
                
            sql_query = (
                         "INSERT INTO tbl_PubMed_Publications (" +
                         sql_query_fields +
                         ") VALUES (" +
                         sql_query_values +
                         ");")
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            self.run_sql_query("SELECT Last_Insert_ID() AS Publication_ID;")
            db_result = self.database_cursor.fetchone()
            if db_result:
                return_publication_id = int(db_result[0])
                
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
            sql_query += " UCase(Trim(%s)) = UCase(Trim(LastName)) "
            db_parameters.append(last_name_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " LastName IS NULL "
            
        if first_name_in:
            sql_query += " AND UCase(Trim(%s)) = UCase(Trim(FirstName)) "
            db_parameters.append(first_name_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " AND FirstName IS NULL "
            
        if initials_in:
            sql_query += " AND UCase(Trim(%s)) = UCase(Trim(Initials)) "
            db_parameters.append(initials_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " AND Initials IS NULL "
            
        if affiliation_in:
            sql_query += " AND UCase(Trim(%s)) = UCase(Trim(Affiliation)) "
            db_parameters.append(affiliation_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " AND Affiliation IS NULL "
            
        sql_query += ";"
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchall()
        
        if db_result:
            return_coauthor_id = int(db_result[0][0])
        
        if not return_coauthor_id:
            
            sql_query_fields = "LastName"
            sql_query_values = "%s"
            
            db_parameters = []
            
            db_parameters.append(last_name_in.encode('ascii', 'xmlcharrefreplace'))
            
            if first_name_in:
                sql_query_fields += ", FirstName"
                sql_query_values += ", %s"
                db_parameters.append(first_name_in.encode('ascii', 'xmlcharrefreplace'))
            
            if initials_in:
                sql_query_fields += ", Initials"
                sql_query_values += ", %s"
                db_parameters.append(initials_in.encode('ascii', 'xmlcharrefreplace'))
            
            if affiliation_in:
                sql_query_fields += ", Affiliation"
                sql_query_values += ", %s"
                db_parameters.append(affiliation_in.encode('ascii', 'xmlcharrefreplace'))
                
            self.run_sql_query("INSERT INTO tbl_Coauthors (" + sql_query_fields + ") VALUES (" + sql_query_values + ");", db_parameters)
            self.database_connection.commit()
            
            self.run_sql_query("SELECT Last_Insert_ID() AS Coauthor_ID;")
            db_result = self.database_cursor.fetchone()
            if db_result:
                return_coauthor_id = int(db_result[0])
        
        return return_coauthor_id
    
    def join_publication_coauthor(self, publication_id_in, coauthor_id_in):
        
        db_parameters = []
        db_result = None
        
        check_publication_coauthor = 0
        
        sql_query = """
                       SELECT Count(*) AS Publication_Coauthor_Count
                       FROM tbl_Publication_Coauthors
                       WHERE
                           Publication_ID = %s AND
                           Coauthor_ID = %s
                       ;"""
        
        db_parameters.append(publication_id_in)
        db_parameters.append(coauthor_id_in)
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        if db_result:
            check_publication_coauthor = int(db_result[0])
        
        if check_publication_coauthor == 0:
            
            sql_query = """
                           INSERT INTO tbl_Publication_Coauthors (
                               Publication_ID, Coauthor_ID
                           ) VALUES (
                               %s, %s
                           );"""

            db_parameters = []
            
            db_parameters.append(publication_id_in)
            db_parameters.append(coauthor_id_in)
            
            self.run_sql_query(sql_query, db_parameters)
        
    def get_mesh_descriptor_id (self, mesh_descriptor_in):

        db_result = None
        mesh_descriptor_id = None
        
        sql_query = """
                       SELECT Mesh_Descriptor_ID
                       FROM tbl_Mesh_Descriptors
                       WHERE UCase(Trim(Mesh_Descriptor)) = UCase(Trim(%s))
                       ;"""

        self.run_sql_query(sql_query, (mesh_descriptor_in,))
        db_result = self.database_cursor.fetchone()
        if db_result:
            mesh_descriptor_id = int(db_result[0])
        
        if mesh_descriptor_id == None:

            self.run_sql_query("INSERT INTO tbl_Mesh_Descriptors (Mesh_Descriptor) VALUES (%s);", (mesh_descriptor_in,))
            self.database_connection.commit()
            
            self.run_sql_query("SELECT Last_Insert_ID() AS Mesh_Descriptor_ID;")
            db_result = self.database_cursor.fetchone()
            if db_result:
                mesh_descriptor_id = int(db_result[0])
            
        return mesh_descriptor_id
        
    def get_mesh_qualifier_id (self, mesh_qualifier_in):
        
        db_result = None
        mesh_qualifier_id = None
        
        sql_query = """
                       SELECT Mesh_Qualifier_ID
                       FROM tbl_Mesh_Qualifiers
                       WHERE UCase(Trim(Mesh_Qualifier)) = UCase(Trim(%s))
                       ;"""
        
        self.run_sql_query(sql_query, (mesh_qualifier_in,))
        db_result = self.database_cursor.fetchone()
        if db_result:
            mesh_qualifier_id = int(db_result[0])
        
        if mesh_qualifier_id == None:
            
            self.run_sql_query("INSERT INTO tbl_Mesh_Qualifiers (Mesh_Qualifier) VALUES (%s);", (mesh_qualifier_in,))
            self.database_connection.commit()
            
            self.run_sql_query("SELECT Last_Insert_ID() AS Mesh_Qualifier_ID;")
            db_result = self.database_cursor.fetchone()
            if db_result:
                mesh_qualifier_id = int(db_result[0])
            
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
                           Publication_ID = %s AND
                           Mesh_Descriptor_ID = %s """

        db_parameters = []
        
        db_parameters.append(publication_id_in)
        db_parameters.append(descriptor_id_in)
                           
        if qualifier_id_in != None:
            sql_query += " AND Mesh_Qualifier_ID = %s "
            db_parameters.append(qualifier_id_in)
        else:
            sql_query += " AND Mesh_Qualifier_ID IS NULL "
        
        sql_query += ";"
            
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        if db_result:
            publication_mesh_term_id = int(db_result[0])
            
        if publication_mesh_term_id == None:
            
            sql_query_fields = "Publication_ID, Mesh_Descriptor_ID"
            sql_query_values = "%s, %s"
            
            db_parameters = []

            db_parameters.append(publication_id_in)
            db_parameters.append(descriptor_id_in)

            if descriptor_major_topic_in:
                sql_query_fields += ", Major_Descriptor"
                sql_query_values += ", %s"
                db_parameters.append(descriptor_major_topic_in)
            
            if qualifier_id_in != None:
                sql_query_fields += ", Mesh_Qualifier_ID"
                sql_query_values += ", %s"
                db_parameters.append(qualifier_id_in)
                
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier"
                sql_query_values += ", %s"
                db_parameters.append(qualifier_major_topic_in)
            
            self.run_sql_query("INSERT INTO tbl_Publication_Mesh_Terms (" + sql_query_fields + ") VALUES (" + sql_query_values + ");", db_parameters)
            self.database_connection.commit()
            
        else:
            
            sql_query_fields = "Major_Descriptor = %s"

            db_parameters = []
            
            db_parameters.append(descriptor_major_topic_in)
            
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier = %s"
                db_parameters.append(qualifier_major_topic_in)
            
            db_parameters.append(publication_mesh_term_id)
            
            self.run_sql_query("UPDATE tbl_Publication_Mesh_Terms SET " + sql_query_fields + " WHERE Publication_Mesh_Term_ID = %s;", db_parameters)
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
                     actsi_grant_in):
        
        db_result = None
        return_grant_id = None
        
        sql_query = "SELECT Grant_ID FROM tbl_Grants WHERE Grant_Number = %s "

        db_parameters = []
        
        if grant_number_in:
            db_parameters.append(grant_number_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            db_parameters.append(None)
                           
        if phs_activity_code_in != None:
            sql_query += " AND PHS_Activity_Code = %s "
            db_parameters.append(phs_activity_code_in)
        else:
            sql_query += " AND PHS_Activity_Code IS NULL "
                           
        if phs_organization_in != None:
            sql_query += " AND PHS_Organization = %s "
            db_parameters.append(phs_organization_in)
        else:
            sql_query += " AND PHS_Organization IS NULL "
                           
        if phs_grant_number_in != None:
            sql_query += " AND PHS_Six_Digit_Grant_Number = %s "
            db_parameters.append(phs_grant_number_in)
        else:
            sql_query += " AND PHS_Six_Digit_Grant_Number IS NULL "
                           
        if agency_in != None:
            sql_query += " AND Agency = %s "
            db_parameters.append(agency_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " AND Agency IS NULL "
                           
        if country_in != None:
            sql_query += " AND Country = %s "
            db_parameters.append(country_in.encode('ascii', 'xmlcharrefreplace'))
        else:
            sql_query += " AND Country IS NULL "
        
        sql_query += ";"
            
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        
        if db_result:
            return_grant_id = int(db_result[0])
            
        if return_grant_id == None:
            
            sql_query_fields = "Grant_Number, Acronym, Agency, Country"
            sql_query_values = "%s, %s, %s, %s"

            db_parameters = []
            
            if grant_number_in:
                db_parameters.append(grant_number_in.encode('ascii', 'xmlcharrefreplace'))
            else:
                db_parameters.append(None)
                
            db_parameters.append(acronym_in)
            db_parameters.append(agency_in.encode('ascii', 'xmlcharrefreplace'))
            db_parameters.append(country_in.encode('ascii', 'xmlcharrefreplace'))
            
            if phs_activity_code_in != None:
                sql_query_fields += ", PHS_Activity_Code"
                sql_query_values += ", %s"
                db_parameters.append(phs_activity_code_in)
                
            if phs_organization_in != None:
                sql_query_fields += ", PHS_Organization"
                sql_query_values += ", %s"
                db_parameters.append(phs_organization_in)
                
            if phs_grant_number_in != None:
                sql_query_fields += ", PHS_Six_Digit_Grant_Number"
                sql_query_values += ", %s"
                db_parameters.append(phs_grant_number_in)
            
            if actsi_grant_in != None:
                sql_query_fields += ", CTSA_Grant_Number_ID"
                sql_query_values += ", %s"
                db_parameters.append(actsi_grant_in)
            
            self.run_sql_query("INSERT INTO tbl_Grants (" + sql_query_fields + ") VALUES (" + sql_query_values + ");", db_parameters)
            self.database_connection.commit()
            
            self.run_sql_query("SELECT Last_Insert_ID() AS Grant_ID;")
            db_result = self.database_cursor.fetchone()
            if db_result:
                return_grant_id = int(db_result[0])
            
        else:
            
            sql_query_fields = "Acronym = %s, Agency = %s, Country = %s"

            db_parameters = []
            
            db_parameters.append(acronym_in)
            db_parameters.append(agency_in.encode('ascii', 'xmlcharrefreplace'))
            db_parameters.append(country_in.encode('ascii', 'xmlcharrefreplace'))
            
            if actsi_grant_in != None:
                sql_query_fields += ", CTSA_Grant_Number_ID = %s"
                db_parameters.append(actsi_grant_in)
            
            db_parameters.append(return_grant_id)
            
            self.run_sql_query("UPDATE tbl_Grants SET " + sql_query_fields + " WHERE Grant_ID = %s;", db_parameters)
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
                           PHS_Organization = %s AND
                           PHS_Six_Digit_Grant_Number = %s
                       ;"""
                       
        self.run_sql_query(sql_query, (phs_organization_in, phs_grant_number_in.zfill(6)))
        rows = self.database_cursor.fetchall()
        
        if rows:
            return_grant_id = int(rows[0][0])
        
        return return_grant_id
    
    def join_publication_grant(self, publication_id_in, grant_id_in):
        
        db_result = None
        publication_grants_cited_id = None
        
        sql_query = """
                       SELECT
                           Publication_Grants_Cited_ID
                       FROM tbl_Publication_Grants_Cited
                       WHERE
                           Publication_ID = %s AND
                           Grant_ID = %s
                       ;"""

        db_parameters = []
        
        db_parameters.append(publication_id_in)
        db_parameters.append(grant_id_in)
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        if db_result:
            publication_grants_cited_id = int(db_result[0])
            
        if publication_grants_cited_id == None:
            
            sql_query = """
                           INSERT INTO tbl_Publication_Grants_Cited (
                               Publication_ID, Grant_ID
                           ) VALUES (
                               %s, %s
                           );"""

            db_parameters = []
        
            db_parameters.append(publication_id_in)
            db_parameters.append(grant_id_in)
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
    
    def join_investigator_publication(self, investigator_id_in, publication_id_in):

        db_result = None
        check_investigator_publication = 0
        
        sql_query = """
                       SELECT count(*) AS Investigator_Publication_Count
                       FROM tbl_Investigator_Publications
                       WHERE
                           Investigator_ID = %s AND
                           Publication_ID = %s
                       ;"""
        
        db_parameters = []
        
        db_parameters.append(investigator_id_in)
        db_parameters.append(publication_id_in)
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        if db_result:
            check_investigator_publication = int(db_result[0])
        
        if check_investigator_publication == 0:
            
            sql_query = """
                           INSERT INTO tbl_Investigator_Publications (
                               Investigator_ID, Publication_ID
                           ) VALUES (
                               %s, %s
                           );"""

            db_parameters = []
            
            db_parameters.append(investigator_id_in)
            db_parameters.append(publication_id_in)
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(publication_id_in) + ' to Investigator ID ' + str(investigator_id_in))
    
    def join_investigator_coauthor_searched(self, investigator_id_in, coauthor_id_in):
        
        db_result = None
        check_investigator_coauthor = 0
        
        sql_query = """
                       SELECT count(*) AS Investigator_Coauthor_Count
                       FROM tbl_Investigator_Coauthors_Searched
                       WHERE
                           Investigator_ID = %s AND
                           Coauthor_ID = %s
                       ;"""

        db_parameters = []
        
        db_parameters.append(investigator_id_in)
        db_parameters.append(coauthor_id_in)
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        if db_result:
            check_investigator_coauthor = int(db_result[0])
        
        if check_investigator_coauthor == 0:
            
            sql_query = """
                           INSERT INTO tbl_Investigator_Coauthors_Searched (
                               Investigator_ID, Coauthor_ID
                           ) VALUES (
                               %s, %s
                           );"""

            db_parameters = []
        
            db_parameters.append(investigator_id_in)
            db_parameters.append(coauthor_id_in)
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
    
    def join_pmcid_cited_by_pmcid(self, pmcid_cited_publication_id_in, pmcid_cited_by_publication_id_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT COUNT(*) AS PMCID_to_PMCID_Citation_Count
                       FROM tbl_PMCID_to_PMCID_Citations
                       WHERE
                           PMCID_Cited_Publication_ID = %s AND
                           PMCID_Cited_by_Publication_ID = %s
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pmcid_cited_publication_id_in))
        db_parameters.append(int(pmcid_cited_by_publication_id_in))
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result[0])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PMCID_to_PMCID_Citations (
                               PMCID_Cited_Publication_ID, PMCID_Cited_by_Publication_ID
                           ) VALUES (
                               %s, %s
                           );"""
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pmcid_cited_publication_id_in) + ' cited by Publication ID ' + str(pmcid_cited_by_publication_id_in) + ' in PubMed Central')
    
    def join_pubmed_id_cited_by_pubmed_id(self, pubmed_id_cited_publication_id_in, pubmed_id_cited_by_publication_id_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT COUNT(*) AS PubMed_ID_to_PubMed_ID_Citation_Count
                       FROM tbl_PubMed_ID_to_PubMed_ID_Citations
                       WHERE
                           PubMed_ID_Cited_Publication_ID = %s AND
                           PubMed_ID_Cited_by_Publication_ID = %s
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pubmed_id_cited_publication_id_in))
        db_parameters.append(int(pubmed_id_cited_by_publication_id_in))
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result[0])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PubMed_ID_to_PubMed_ID_Citations (
                               PubMed_ID_Cited_Publication_ID, PubMed_ID_Cited_by_Publication_ID
                           ) VALUES (
                               %s, %s
                           );"""
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pubmed_id_cited_publication_id_in) + ' cited by Publication ID ' + str(pubmed_id_cited_by_publication_id_in) + ' in PubMed')
    
    def join_pubmed_id_neighbor_pubmed_id(self, pubmed_id_publication_id_in, pubmed_id_neighbor_in, neighbor_score_in):
        
        db_result = None
        check_pmcid_to_pmcid_citation = 0
        
        sql_query = """
                       SELECT COUNT(*) AS PubMed_ID_to_PubMed_ID_Neighbor_Count
                       FROM tbl_PubMed_ID_to_PubMed_ID_Neighbors
                       WHERE
                           Publication_ID = %s AND
                           Neighbor_PubMed_ID = %s
                       ;"""

        db_parameters = []
        
        db_parameters.append(int(pubmed_id_publication_id_in))
        db_parameters.append(pubmed_id_neighbor_in)
        
        self.run_sql_query(sql_query, db_parameters)
        db_result = self.database_cursor.fetchone()
        
        if db_result:
            check_pmcid_to_pmcid_citation = int(db_result[0])
        
        if check_pmcid_to_pmcid_citation == 0:
            
            sql_query = """
                           INSERT INTO tbl_PubMed_ID_to_PubMed_ID_Neighbors (
                               Publication_ID, Neighbor_PubMed_ID, Neighbor_Score
                           ) VALUES (
                               %s, %s, %s
                           );"""
            
            db_parameters.append(neighbor_score_in)
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Added Publication ID ' + str(pubmed_id_publication_id_in) + ' neighbor to PubMed ID ' + str(pubmed_id_neighbor_in))
            
        else:

            db_parameters = []
            
            sql_query = """
                           UPDATE tbl_PubMed_ID_to_PubMed_ID_Neighbors
                           SET
                               Neighbor_Score = %s
                           WHERE
                               Publication_ID = %s AND
                               Neighbor_PubMed_ID = %s
                           ;"""
            
            db_parameters.append(neighbor_score_in)
            db_parameters.append(int(pubmed_id_publication_id_in))
            db_parameters.append(pubmed_id_neighbor_in)
            
            self.run_sql_query(sql_query, db_parameters)
            self.database_connection.commit()
            
            print('Updated Publication ID ' + str(pubmed_id_publication_id_in) + ' neighbor to PubMed ID ' + str(pubmed_id_neighbor_in))
        
    def get_pmcid_from_pubmed_id(self, pubmed_id_in):
        
        db_result = None
        pmcid_result = None
        
        sql_query = """
                       SELECT PMCID
                       FROM tbl_PubMed_Publications
                       WHERE TRIM(PubMed_ID) = TRIM(%s)
                       ;"""
        
        self.run_sql_query(sql_query, (pubmed_id_in, ))
        db_result = self.database_cursor.fetchone()
        
        if db_result:
            
            try:
                pmcid_result = db_result[0].strip()
            except AttributeError:
                pass
            
        return pmcid_result

