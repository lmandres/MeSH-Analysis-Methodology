'''
Created on Aug 26, 2011

@author: Leo Andres (user)
'''

import os
import re

import searchlib.helper
    
class PubMedSearchDatabaseController:
    
    PERSON_BY_PERSON = 2**0
    PERSON_ORGANIZATION = 2**1
    PERSON_GRANT = 2**2
    PERSON_COAUTHOR = 2**3
    CTSA_GRANT = 2**4
    PMCID_CITE_BY_PMCID = 2**5
    PUBMED_ID_CITE_BY_PUBMED_ID = 2**6
    PUBMED_ID_NEIGHBOR_PUBMED_ID = 2**7
    
    phs_activity_codes = None
    phs_organizations = None
    
    database_driver = None
    database_path = None
    
    connection_type = None
    connection_properties = {}
    
    database_manager = None
    database_search = None
    
    search_strategy_flags = 0
    search_strategy_running_flags = 0
    
    pubmed_parser = None
    pubmed_article_xml = None
    
    def __init__(self):
        
        phs_activity_codes_filename = os.path.abspath('phsActivityCodes.txt')
        phs_organization_codes_filename = os.path.abspath('phsOrganizations.txt')
        
        phs_activity_codes_file_in = open(phs_activity_codes_filename, 'r')
        phs_organizations_codes_file_in = open(phs_organization_codes_filename, 'r')
        
        self.phs_activity_codes = '|'.join([file_line.strip() for file_line in phs_activity_codes_file_in])
        self.phs_organizations = '|'.join([file_line.strip() for file_line in phs_organizations_codes_file_in])
        
    def __get_current_running_flag(self, current_running_flags_in):
        
        current_running_flag = 0
            
        for running_flag_index in range(0, 5, 1):
            
            if (2**running_flag_index & current_running_flags_in) > 0:
                current_running_flag = 2**running_flag_index
                break
            
        return current_running_flag
        
    def get_connection_type(self):
        return self.connection_type
        
    def set_connection_type(self, connection_type_in):
        self.connection_type = connection_type_in
        
    def get_connection_properties(self):
        return self.connection_properties
        
    def set_connection_properties(self, connection_properties_in):
        self.connection_properties = connection_properties_in
        
    def open_database(self):
        
        successful_open = False
        
        database_connection_type_case = None
        try:
            database_connection_type_case = self.connection_type.strip().upper()
        except TypeError:
            pass
        
        if database_connection_type_case == 'JDBCODBCDRIVER':

            import searchlib.jdbcodbcdriver
            
            database_connection_string = None
            
            database_odbc_driver_case = None
            try:
                database_odbc_driver_case = self.connection_properties['driver'].strip().upper()
                database_connection_string = 'DRIVER={' + self.connection_properties['driver'].strip() + '}; DBQ=' + self.connection_properties['dbq'].strip() + ';'
            except TypeError:
                pass
            
            if database_odbc_driver_case == 'MICROSOFT ACCESS DRIVER (*.MDB, *.ACCDB)':
                self.database_manager = searchlib.jdbcodbcdriver.PubMedAccessJDBCODBCDatabaseManager()
                self.database_search = searchlib.jdbcodbcdriver.PubMedAccessJDBCODBCDatabaseSearch()
            
            successful_open = self.database_manager.open_database(database_connection_string)
            if successful_open:
                successful_open = self.database_search.open_database(database_connection_string)
                
        elif database_connection_type_case == 'PYODBCDRIVER':

            import searchlib.pyodbcdriver
            
            database_connection_string = None
            
            database_odbc_driver_case = None
            try:
                database_odbc_driver_case = self.connection_properties['driver'].strip().upper()
            except TypeError:
                pass
            
            if database_odbc_driver_case == 'MICROSOFT ACCESS DRIVER (*.MDB, *.ACCDB)':
                
                try:
                    database_connection_string = 'DRIVER={' + self.connection_properties['driver'].strip() + '}; DBQ=' + self.connection_properties['dbq'].strip() + ';'
                except TypeError:
                    pass
                    
                self.database_manager = searchlib.pyodbcdriver.PubMedAccessPyODBCDatabaseManager()
                self.database_search = searchlib.pyodbcdriver.PubMedAccessPyODBCDatabaseSearch()
                
            elif database_odbc_driver_case == 'SQL SERVER':
                
                try:
                    database_connection_string = 'DRIVER={' + self.connection_properties['driver'].strip() + '}; SERVER=' + self.connection_properties['server'].strip() + '; DATABASE=' + self.connection_properties['database'].strip() + '; TRUSTED_CONNECTION=' + self.connection_properties['trusted_connection'].strip() + ';'
                except TypeError:
                    
                    try:
                        database_connection_string = 'DRIVER={' + self.connection_properties['driver'].strip() + '}; SERVER=' + self.connection_properties['server'].strip() + '; DATABASE=' + self.connection_properties['database'].strip() + '; UID=' + self.connection_properties['uid'] + '; PWD=' + self.connection_properties['pwd'] + ';'
                    except TypeError:
                        pass
                    
                self.database_manager = searchlib.pyodbcdriver.PubMedMSSQLPyODBCDatabaseManager()
                self.database_search = searchlib.pyodbcdriver.PubMedMSSQLPyODBCDatabaseSearch()
            
            successful_open = self.database_manager.open_database(database_connection_string)
            if successful_open:
                successful_open = self.database_search.open_database(database_connection_string)
                
                
        elif database_connection_type_case == 'PYPYODBCDRIVER':

            import searchlib.pypyodbcdriver
            
            database_connection_string = None
            
            database_odbc_driver_case = None
            try:
                database_odbc_driver_case = self.connection_properties['driver'].strip().upper()
                database_connection_string = 'DRIVER={' + self.connection_properties['driver'].strip() + '}; DBQ=' + self.connection_properties['dbq'].strip() + ';'
            except TypeError:
                pass
            
            if database_odbc_driver_case == 'MICROSOFT ACCESS DRIVER (*.MDB, *.ACCDB)':
                self.database_manager = searchlib.pypyodbcdriver.PubMedAccessPyPyODBCDatabaseManager()
                self.database_search = searchlib.pypyodbcdriver.PubMedAccessPyPyODBCDatabaseSearch()
            
            successful_open = self.database_manager.open_database(database_connection_string)
            if successful_open:
                successful_open = self.database_search.open_database(database_connection_string)
                
        elif database_connection_type_case == 'SQLITE3DRIVER':

            import searchlib.sqlite3driver
            
            database_filename = None
            try:
                database_filename = self.connection_properties['filename'].strip()
            except TypeError:
                pass
            
            self.database_manager = searchlib.sqlite3driver.PubMedSQLite3DatabaseManager()
            self.database_search = searchlib.sqlite3driver.PubMedSQLite3DatabaseSearch()
            
            successful_open = self.database_manager.open_database(database_filename)

            if successful_open:
                successful_open = self.database_search.open_database(database_filename)
                
        elif database_connection_type_case == 'MYSQLDRIVER':

            import searchlib.mysqldriver
            
            self.database_manager = searchlib.mysqldriver.PubMedMySQLDatabaseManager()
            self.database_search = searchlib.mysqldriver.PubMedMySQLDatabaseSearch()
        
            host_out = self.connection_properties['host'].strip()
            user_out = self.connection_properties['user'].strip()
            passwd_out = self.connection_properties['passwd'].strip()
            db_out = self.connection_properties['db'].strip()
            
            successful_open = self.database_manager.open_database(host = host_out, user = user_out, password = passwd_out, database = db_out)
            if successful_open:
                successful_open = self.database_search.open_database(host = host_out, user = user_out, password = passwd_out, database = db_out)
            
        return successful_open
    
    def close_database(self):
        try:
            return self.database_manager.close_database()
        except AttributeError:
            return False
    
    def reset_publications_tables(self):
        self.database_manager.delete_publications_tables()
        self.database_manager.create_publications_tables()
    
    def filter_missing_pubmed_ids(self, pubmed_id_iterable_in):

        class IterateFilterMissingPubMedIDs:
            
            database_manager = None
            pubmed_id_iterable = None
            
            def __init__(self, database_manager_in, pubmed_id_iterable_in):
                
                self.database_manager = database_manager_in
                self.pubmed_id_iterable = None
                
                try:
                    self.pubmed_id_iterable = pubmed_id_iterable_in.__iter__()
                except AttributeError:
                    pass
                
            def __iter__(self):
                return self
            
            def __next__(self):
                
                while True:
                    
                    pubmed_id = None
                    
                    try:
                        pubmed_id = self.pubmed_id_iterable.__next__()
                    except AttributeError:
                        raise StopIteration
                    
                    try:
                        pubmed_id = pubmed_id.strip()
                    except AttributeError:
                        pass
                    
                    if self.database_manager.lookup_publication_id_by_pubmed_id(pubmed_id) == None:
                        return pubmed_id
        
        return IterateFilterMissingPubMedIDs(self.database_manager, pubmed_id_iterable_in)
    
    def set_search_strategy(self, search_flag_in):
        self.search_strategy_flags |= search_flag_in
        
    def unset_search_strategy(self, search_flag_in):
        self.search_strategy_flags &= ~search_flag_in
        
    def get_search_term(self):

        if self.search_strategy_running_flags == 0:
            self.search_strategy_running_flags = self.search_strategy_flags
            self.database_search.set_running_flag(self.__get_current_running_flag(self.search_strategy_running_flags))

        current_running_flag = self.database_search.get_running_flag()
        
        while True:

            return_search_term = self.database_search.get_search_term()
                    
            if return_search_term == None:
                
                if current_running_flag == self.PERSON_COAUTHOR and self.database_search.get_search_recordcount(self.PERSON_COAUTHOR) > 0:
                    current_running_flag = 0
                    
                self.search_strategy_running_flags &= ~current_running_flag
                self.database_search.set_running_flag(self.__get_current_running_flag(self.search_strategy_running_flags))
                
            elif current_running_flag == self.PERSON_COAUTHOR:
                self.database_manager.join_investigator_coauthor_searched(self.database_search.get_person_id(), self.database_search.get_coauthor_id())
                
            if return_search_term != None or self.search_strategy_running_flags == 0:
                break
            
        return return_search_term

    def read_pubmed_article(self, article_xml_in):
    
        def __get_pubmed_id(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><PMID>')
        
        def __get_journal_title(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Journal><Title>')
        
        def __get_pmcid(pubmed_parser_in):
            
            return_pmcid = None
            pmcid_index = 0
            
            while True:
                
                xml_path = '<PubmedArticle><PubmedData><ArticleIdList><ArticleId<' + str(pmcid_index) + '>>'
                
                if pubmed_parser_in.get_string_cdata(xml_path) == None:
                    break
                elif pubmed_parser_in.get_string_attr(xml_path, 'IdType') == 'pmc':
                    return_pmcid = pubmed_parser_in.get_string_cdata(xml_path)
                    break
                
                pmcid_index += 1
            
            return return_pmcid
        
        def __get_article_title(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><ArticleTitle>')
        
        def __get_affiliation(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Affiliation>')
        
        def __get_volume(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Journal><JournalIssue><Volume>')
        
        def __get_issue(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Journal><JournalIssue><Issue>')
        
        def __get_pagination(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Pagination><MedlinePgn>')
        
        def __get_medline_date(pubmed_parser_in):
            return pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Journal><JournalIssue><PubDate><MedlineDate>')
        
        def __get_article_year(pubmed_parser_in):
            
            return_article_year = pubmed_parser_in.get_integer_cdata('<PubmedArticle><MedlineCitation><Article><Journal><JournalIssue><PubDate><Year>')
            
            if return_article_year == None:
                match = re.match('\s*(\d+).*', __get_medline_date(pubmed_parser_in))
                if match:
                    return_article_year = int(match.group(1).strip())
                    
            return return_article_year
        
        def __get_article_month(pubmed_parser_in):
            
            months_dict = {
                           'Jan' : 1,
                           'Feb' : 2,
                           'Mar' : 3,
                           'Apr' : 4,
                           'May' : 5,
                           'Jun' : 6,
                           'Jul' : 7,
                           'Aug' : 8,
                           'Sep' : 9,
                           'Oct' : 10,
                           'Nov' : 11,
                           'Dec' : 12}
            
            return_month = None
            
            try:
                return_month = months_dict[pubmed_parser_in.get_string_cdata('<PubmedArticle><MedlineCitation><Article><Journal><JournalIssue><PubDate><Month>')]
            except KeyError:
                
                try:
                    match = re.match('\s*\d+\s+(\w+)-.*', __get_medline_date(pubmed_parser_in))
                    if match:
                        return_month = months_dict[match.group(1).strip()]
                except KeyError:
                    pass
                except TypeError:
                    pass
                
            except TypeError:
                pass
            
            return return_month
        
        def __get_author_list_complete(pubmed_parser_in):
            
            return_complete = None
            
            try:
                
                if pubmed_parser_in.get_string_attr('<PubmedArticle><MedlineCitation><Article><AuthorList>', 'CompleteYN') == 'Y':
                    return_complete = True
                elif pubmed_parser_in.get_string_attr('<PubmedArticle><MedlineCitation><Article><AuthorList>', 'CompleteYN') == 'N':
                    return_complete = False
                    
            except TypeError:
                pass
            except KeyError:
                pass
            
            return return_complete
        
        def __get_grant_list_complete(pubmed_parser_in):
            
            return_complete = None
            
            try:
                
                if pubmed_parser_in.get_string_attr('<PubmedArticle><MedlineCitation><Article><GrantList>', 'CompleteYN') == 'Y':
                    return_complete = True
                elif pubmed_parser_in.get_string_attr('<PubmedArticle><MedlineCitation><Article><GrantList>', 'CompleteYN') == 'N':
                    return_complete = False
                    
            except TypeError:
                pass
            except KeyError:
                pass
            
            return return_complete
        
        def __get_authors(pubmed_parser_in):
            
            return_authors = []
            author_index = 0
            
            while True:
                
                author_item = ''
                xml_path = '<PubmedArticle><MedlineCitation><Article><AuthorList><Author<' + str(author_index) + '>>'
                
                if pubmed_parser_in.get_element_item(xml_path) == None:
                    break
                
                if pubmed_parser_in.get_string_cdata(xml_path + '<LastName>'):
                    
                    author_item += pubmed_parser_in.get_string_cdata(xml_path + '<LastName>').strip()
                    
                    if pubmed_parser_in.get_string_cdata(xml_path + '<ForeName>'):
                        author_item += ', ' + pubmed_parser_in.get_string_cdata(xml_path + '<ForeName>').strip()
                    elif pubmed_parser_in.get_string_cdata(xml_path + '<FirstName>'):
                        author_item += ', ' + pubmed_parser_in.get_string_cdata(xml_path + '<FirstName>').strip()
                    elif pubmed_parser_in.get_string_cdata(xml_path + '<Initials>'):
                        author_item += ', ' + pubmed_parser_in.get_string_cdata(xml_path + '<Initials>').strip()
                    
                return_authors.append(author_item)
                
                author_index += 1
                
            return '; '.join(return_authors)
        
        def __add_publication_coauthors(pubmed_parser_in, database_manager_in, publication_id_in):
            
            coauthor_index = 0
            xml_path = None
            
            while True:
            
                coauthor_last_name = None
                coauthor_first_name = None
                coauthor_initials = None
                coauthor_affiliation = None
                
                coauthor_id = None
                
                xml_path = '<PubmedArticle><MedlineCitation><Article><AuthorList><Author<' + str(coauthor_index) + '>>'
                
                if pubmed_parser_in.get_element_item(xml_path) == None:
                    break
                
                if pubmed_parser_in.get_string_cdata(xml_path + '<LastName>'):
                
                    coauthor_last_name = pubmed_parser_in.get_string_cdata(xml_path + '<LastName>')
                    
                    if pubmed_parser_in.get_string_cdata(xml_path + '<ForeName>'):
                        coauthor_first_name = pubmed_parser_in.get_string_cdata(xml_path + '<ForeName>')
                    elif pubmed_parser_in.get_string_cdata(xml_path + '<FirstName>'):
                        coauthor_first_name = pubmed_parser_in.get_string_cdata(xml_path + '<FirstName>')
                        
                    coauthor_initials = pubmed_parser_in.get_string_cdata(xml_path + '<Initials>')
                    
                    try:
                        coauthor_affiliation = pubmed_parser_in.get_string_cdata(xml_path + '<Affiliation>').strip()
                    except AttributeError:
                        pass
                    
                    coauthor_id = database_manager_in.get_coauthor_id(coauthor_last_name, coauthor_first_name, coauthor_initials, coauthor_affiliation)
                    database_manager_in.join_publication_coauthor(publication_id_in, coauthor_id)
                
                coauthor_index += 1
                
        def __add_publication_mesh_terms(pubmed_parser_in, database_manager_in, publication_id_in):
            
            mesh_term_index = 0
            mesh_qualifier_index = 0
            
            xml_path = None
            
            while True:
            
                descriptor_major_topic = None
                qualifier_major_topic = None
                
                descriptor_id = None
                qualifier_id = None
                
                xml_path = '<PubmedArticle><MedlineCitation><MeshHeadingList><MeshHeading<' + str(mesh_term_index) + '>>'
                qualifier_id = None
                
                if pubmed_parser_in.get_element_item(xml_path) == None:
                    break
                
                descriptor_id = database_manager_in.get_mesh_descriptor_id(pubmed_parser_in.get_string_cdata(xml_path + '<DescriptorName>'))
                
                if pubmed_parser_in.get_string_attr(xml_path + '<DescriptorName>', 'MajorTopicYN') == 'Y':
                    descriptor_major_topic = True
                elif pubmed_parser_in.get_string_attr(xml_path + '<DescriptorName>', 'MajorTopicYN') == 'N':
                    descriptor_major_topic = False
                    
                mesh_qualifier_index = 0
            
                while True:
                
                    if pubmed_parser_in.get_element_item(xml_path + '<QualifierName<' + str(mesh_qualifier_index) + '>>') == None:
                        break
                    
                    qualifier_id = database_manager_in.get_mesh_qualifier_id(pubmed_parser_in.get_string_cdata(xml_path + '<QualifierName<' + str(mesh_qualifier_index) + '>>'))
                    
                    if pubmed_parser_in.get_string_attr(xml_path + '<QualifierName<' + str(mesh_qualifier_index) + '>>', 'MajorTopicYN') == 'Y':
                        qualifier_major_topic = True
                    elif pubmed_parser_in.get_string_attr(xml_path + '<QualifierName<' + str(mesh_qualifier_index) + '>>', 'MajorTopicYN') == 'N':
                        qualifier_major_topic = False
                    
                    database_manager_in.join_publication_mesh_terms(
                                                                       publication_id_in,
                                                                       descriptor_id,
                                                                       descriptor_major_topic,
                                                                       qualifier_id,
                                                                       qualifier_major_topic)
                    
                    mesh_qualifier_index += 1
                    
                if qualifier_id == None:
                    database_manager_in.join_publication_mesh_terms(
                                                                       publication_id_in,
                                                                       descriptor_id,
                                                                       descriptor_major_topic,
                                                                       None,
                                                                       None)
                    
                mesh_term_index += 1
                
        def __add_publication_grants(pubmed_parser_in, database_manager_in, publication_id_in):
            
            grant_index = 0
            
            xml_path = None
            
            while True:
            
                phs_activity_code = None
                phs_organization = None
                phs_grant_number = None
                
                ctsa_grant_number_id = None
            
                publication_grant_id = None
                
                xml_path = '<PubmedArticle><MedlineCitation><Article><GrantList><Grant<' + str(grant_index) + '>>'
                match = None
                
                if pubmed_parser_in.get_element_item(xml_path) == None:
                    break
            
                if pubmed_parser_in.get_string_cdata(xml_path + '<GrantID>'):
                    match = re.match('.*?(' + self.phs_activity_codes + ')?[\s|-]*?(' + self.phs_organizations + ')[\s|-]*?(\d+).*?', pubmed_parser_in.get_string_cdata(xml_path + '<GrantID>'))
                
                if match:
                    
                    phs_activity_code = match.group(1)
                    phs_organization = match.group(2)
                    phs_grant_number = match.group(3)
                
                    ctsa_grant_number_id = database_manager_in.get_ctsa_grant_id(phs_organization, phs_grant_number)
                
                publication_grant_id = database_manager_in.get_grant_id(
                                                                           pubmed_parser_in.get_string_cdata(xml_path + '<GrantID>'),
                                                                           phs_activity_code,
                                                                           phs_organization,
                                                                           phs_grant_number,
                                                                           pubmed_parser_in.get_string_cdata(xml_path + '<Acronym>'),
                                                                           pubmed_parser_in.get_string_cdata(xml_path + '<Agency>'),
                                                                           pubmed_parser_in.get_string_cdata(xml_path + '<Country>'),
                                                                           ctsa_grant_number_id)
                
                database_manager_in.join_publication_grant(publication_id_in, publication_grant_id)

                grant_index += 1
        
        current_publication_id = None
        current_running_flag = None
        
        self.pubmed_parser = None
        self.pubmed_parser = searchlib.helper.TextXMLParser()
        
        self.pubmed_article_xml = article_xml_in
        self.pubmed_parser.parse_xml_string(self.pubmed_article_xml, True)
        
        current_publication_id = self.database_manager.insert_update_publication(
                                                                                    __get_pubmed_id(self.pubmed_parser),
                                                                                    __get_journal_title(self.pubmed_parser),
                                                                                    __get_pmcid(self.pubmed_parser),
                                                                                    __get_medline_date(self.pubmed_parser),
                                                                                    __get_authors(self.pubmed_parser),
                                                                                    __get_article_title(self.pubmed_parser),
                                                                                    __get_affiliation(self.pubmed_parser),
                                                                                    __get_volume(self.pubmed_parser),
                                                                                    __get_issue(self.pubmed_parser),
                                                                                    __get_pagination(self.pubmed_parser),
                                                                                    __get_article_year(self.pubmed_parser),
                                                                                    __get_article_month(self.pubmed_parser),
                                                                                    __get_author_list_complete(self.pubmed_parser),
                                                                                    __get_grant_list_complete(self.pubmed_parser),
                                                                                    self.pubmed_article_xml.decode('utf-8', 'ignore'))
        
        __add_publication_coauthors(self.pubmed_parser, self.database_manager, current_publication_id)
        __add_publication_mesh_terms(self.pubmed_parser, self.database_manager, current_publication_id)
        __add_publication_grants(self.pubmed_parser, self.database_manager, current_publication_id)
        
        return __get_pubmed_id(self.pubmed_parser)
    
    def join_investigator_pubmed_id(self, person_id_in, pubmed_id_in):
        
        current_publication_id = self.database_manager.lookup_publication_id_by_pubmed_id(pubmed_id_in)
        current_running_flag = self.__get_current_running_flag(self.search_strategy_running_flags)
                            
        if person_id_in and current_publication_id and current_running_flag > 0:
            self.database_manager.join_investigator_publication(person_id_in, current_publication_id)
        
    def get_person_id(self):
        return self.database_search.get_person_id()
    
    def get_coinvestigator_id(self):
        return self.database_search.get_coinvestigator_id()
    
    def get_current_running_flag(self):
        return self.__get_current_running_flag(self.search_strategy_running_flags)
    
    def get_pmcid_from_pubmed_id(self, pubmed_id_in):
        
        pmcid_result = None
        
        try:
            pmcid_result = self.database_manager.get_pmcid_from_pubmed_id(pubmed_id_in).strip()
            if pmcid_result.startswith('PMC'):
                pmcid_result = pmcid_result[3:]
        except TypeError:
            if pmcid_result[:3] == b'PMC':
                pmcid_result = pmcid_result[3:]
        except AttributeError:
            pass
        
        return pmcid_result
    
    def join_pmcid_cited_by_pmcid(self, pmcid_cited_pubmed_id_in, pmcid_cited_by_pubmed_id_in):

        publication_id_cited = self.database_manager.lookup_publication_id_by_pubmed_id(pmcid_cited_pubmed_id_in)
        publication_id_cited_by = self.database_manager.lookup_publication_id_by_pubmed_id(pmcid_cited_by_pubmed_id_in)
        
        if publication_id_cited and publication_id_cited_by and (publication_id_cited != publication_id_cited_by):
            self.database_manager.join_pmcid_cited_by_pmcid(publication_id_cited, publication_id_cited_by)
    
    def join_pubmed_id_cited_by_pubmed_id(self, pubmed_id_cited_in, pubmed_id_cited_by_in):

        publication_id_cited = self.database_manager.lookup_publication_id_by_pubmed_id(pubmed_id_cited_in)
        publication_id_cited_by = self.database_manager.lookup_publication_id_by_pubmed_id(pubmed_id_cited_by_in)

        if publication_id_cited and publication_id_cited_by and (publication_id_cited != publication_id_cited_by):
            self.database_manager.join_pubmed_id_cited_by_pubmed_id(publication_id_cited, publication_id_cited_by)
    
    def join_pubmed_id_neighbor_pubmed_id(self, pubmed_id_in, pubmed_id_neighbor_in, neighbor_score_in):
        
        if pubmed_id_in and pubmed_id_neighbor_in and (pubmed_id_in != pubmed_id_neighbor_in):
            self.database_manager.join_pubmed_id_neighbor_pubmed_id(self.database_manager.lookup_publication_id_by_pubmed_id(pubmed_id_in), pubmed_id_neighbor_in, neighbor_score_in)