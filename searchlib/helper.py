'''
Created on Jul 5, 2011

@author: Leo Andres (user)
'''

from datetime import timedelta

import os
import re
import xml.parsers.expat

class TextXMLParser:

    element_path = ''
    element_dictionary = {}
    
    current_element_data = {}

    def __init__(self):
        
        self.element_path = ''
        self.element_dictionary = {}
        
        self.current_element_data = {}
        
    def __element_path_to_list(self, element_path_in):
        
        element_indexes = {
                           'path_list' : [],
                           'path_indexes' : []}
        
        for match_item in re.finditer('<(.*?)(<\d*?>)?>', element_path_in):
            
            element_indexes['path_list'].append(match_item.group(1))
            
            path_index = None
            
            try:
                path_index = int(match_item.group(2).strip()[1:len(match_item.group(2).strip())-1])
            except AttributeError:
                pass
            except ValueError:
                pass
            
            element_indexes['path_indexes'].append(path_index)
            
        return element_indexes
    
    def __append_element_data(self, element_path_in, element_attributes_in, element_data_in):
        
        def __insert_item(
                          path_list_in,
                          path_indexes_in,
                          element_dictionary_in,
                          element_attr_in,
                          element_cdata_in):
            
            element_dictionary_out = element_dictionary_in
            
            try:
                
                path_index = len(element_dictionary_in[path_list_in[0]])-1
                
                try:
                    path_index = int(path_indexes_in[0])
                except TypeError:
                    pass
                except ValueError:
                    pass
            
                if len(path_list_in) == 1:
                    
                    element_dictionary_out[path_list_in[0]][path_index]['attributes'] = element_attr_in
                    element_dictionary_out[path_list_in[0]][path_index]['character_data'] = element_cdata_in
                                        
                else:
                    
                    element_dictionary_out[path_list_in[0]][path_index]['sub_elements'] = __insert_item(
                                                                                                        path_list_in[1:],
                                                                                                        path_indexes_in[1:],
                                                                                                        element_dictionary_out[path_list_in[0]][path_index]['sub_elements'],
                                                                                                        element_attr_in,
                                                                                                        element_cdata_in)
                
            except IndexError:
                return None
            except KeyError:
                return None
                                                               
            return element_dictionary_out
        
        self.element_dictionary = __insert_item(
                                                self.__element_path_to_list(element_path_in)['path_list'],
                                                self.__element_path_to_list(element_path_in)['path_indexes'],
                                                self.element_dictionary,
                                                element_attributes_in,
                                                element_data_in)
            
    def __append_element_dict(self, element_path_in):
        
        def __insert_sub_element_dict(
                                      path_list_in,
                                      element_dictionary_in):
            
            element_dictionary_out = element_dictionary_in
            
            if path_list_in[0] not in element_dictionary_out.keys():
                element_dictionary_out[path_list_in[0]] = []
            
            if len(path_list_in) == 1:
                element_dictionary_out[path_list_in[0]].append({
                                                                'attributes' : {},
                                                                'character_data' : [],
                                                                'sub_elements' : {}})
                    
            else:
            
                path_index = len(element_dictionary_out[path_list_in[0]])-1
                
                if len(element_dictionary_out[path_list_in[0]]) <= 0 or 'sub_elements' not in element_dictionary_out[path_list_in[0]][path_index].keys():
                    element_dictionary_out[path_list_in[0]].append({
                                                                    'attributes' : {},
                                                                    'character_data' : [],
                                                                    'sub_elements' : {}})
                    
                element_dictionary_out[path_list_in[0]][path_index]['sub_elements'] = __insert_sub_element_dict(
                                                                                                                path_list_in[1:],
                                                                                                                element_dictionary_out[path_list_in[0]][path_index]['sub_elements'])
        
            return element_dictionary_out
        
        self.element_dictionary = __insert_sub_element_dict(
                                                            self.__element_path_to_list(element_path_in)['path_list'],
                                                            self.element_dictionary)
        
    def __start_element_handler(self, element_name, element_attributes):
        
        if self.element_path == '':
            self.element_dictionary = {}
            self.current_element_data = {}
        
        self.element_path += '<' + element_name.strip() + '>'
        self.__append_element_dict(self.element_path)
        
        self.current_element_data[self.element_path] = {
                                                    'attrs' : {},
                                                    'cdata' : []}
        
        if element_attributes:
            self.current_element_data[self.element_path]['attrs'] = element_attributes
    
    def __end_element_handler(self, element_name):
        
        if self.current_element_data[self.element_path]['attrs'] or self.current_element_data[self.element_path]['cdata']:
            
            self.__append_element_data(
                                       self.element_path,
                                       self.current_element_data[self.element_path]['attrs'],
                                       self.current_element_data[self.element_path]['cdata'])
            
        del(self.current_element_data[self.element_path])
        
        if self.element_path.endswith('<' + element_name.strip() + '>'):
            self.element_path = self.element_path[:self.element_path.rfind('<' + element_name.strip() + '>')]
            
    def __character_data_handler(self, element_data):
        if element_data.strip():
            self.current_element_data[self.element_path]['cdata'].append(element_data.strip())
    
    def parse_xml_file(self, xml_file_in):

        self.element_path = ''
        self.element_dictionary = {}
        
        self.current_element_data = {}

        parser = xml.parsers.expat.ParserCreate()

        parser.StartElementHandler = self.__start_element_handler
        parser.EndElementHandler = self.__end_element_handler
        parser.CharacterDataHandler = self.__character_data_handler

        parser.ParseFile(xml_file_in)
   
    def parse_xml_string(self, xml_string_in, is_final = True):

        self.element_path = ''
        self.element_dictionary = {}
        
        self.current_element_data = {}

        parser = xml.parsers.expat.ParserCreate()

        parser.StartElementHandler = self.__start_element_handler
        parser.EndElementHandler = self.__end_element_handler
        parser.CharacterDataHandler = self.__character_data_handler

        parser.Parse(xml_string_in, is_final)
    
    def get_element_item(self, element_path_in):
        
        def __retrieve_item(path_list_in, path_indexes_in, element_dictionary_in):
            
            try:
                
                path_index = len(element_dictionary_in[path_list_in[0]])-1
                
                try:
                    path_index = int(path_indexes_in[0])
                except TypeError:
                    pass
                except ValueError:
                    pass
                
                if len(path_list_in) == 1:
                    return element_dictionary_in[path_list_in[0]][path_index]
                else:
                    return __retrieve_item(
                                           path_list_in[1:],
                                           path_indexes_in[1:],
                                           element_dictionary_in[path_list_in[0]][path_index]['sub_elements'])
                
            except IndexError:
                return None
            except KeyError:
                return None
        
        return __retrieve_item(
                               self.__element_path_to_list(element_path_in)['path_list'],
                               self.__element_path_to_list(element_path_in)['path_indexes'],
                               self.element_dictionary)
    
    def get_string_cdata(self, element_path_in):
        
        element_value = None
        
        try:
            element_value_list = self.get_element_item(element_path_in)['character_data']
            element_value = ''.join(element_value_list)
        except TypeError:
            pass
        except IndexError:
            pass
        
        return element_value
    
    def get_integer_cdata(self, element_path_in):
        
        element_value = None
        
        try:
            element_value = int(self.get_string_cdata(element_path_in))
        except TypeError:
            pass
        except IndexError:
            pass
        
        return element_value
    
    def get_string_attr(self, element_path_in, element_attr_in):
        
        element_value = None
        
        try:
            element_value = self.get_element_item(element_path_in)['attributes'][element_attr_in]
        except TypeError:
            pass
        except KeyError:
            pass
        
        return element_value
    
    def get_integer_attr(self, element_path_in, element_attr_in):
        
        element_value = None
        
        try:
            element_value = int(self.get_string_attr(element_path_in, element_attr_in))
        except ValueError:
            pass
        
        return element_value

class PubMedSearchSettings:
    '''
    classdocs
    '''
    
    settings_filename = os.path.abspath('pubmed_conf.xml')
    search_settings = None
    
    def __init__(self):
        '''
        Constructor
        '''
        
        self.read_settings()

    def read_settings(self):
        
        self.search_settings = TextXMLParser()
        
        file_in = open(self.settings_filename, 'rb')
        self.search_settings.parse_xml_file(file_in)
        file_in.close()
    
    def get_database_connection_type(self):
        return self.search_settings.get_string_attr('<BiblioAnalysisSettings><DatabaseConnection>', 'type')
    
    def get_database_connection_properties(self):
        
        database_connection_properties = {}
        
        database_connection_type_case = None
        try:
            database_connection_type_case = self.get_database_connection_type().upper()
        except TypeError:
            pass
        
        if database_connection_type_case == 'JDBCODBCDRIVER':
            
            database_odbc_dbq = None
            database_odbc_driver = None
            
            try:
                database_odbc_driver = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Driver>').strip()
            except TypeError:
                pass
            
            try:
                database_odbc_dbq = os.path.abspath(self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><DBQ>').strip())
            except TypeError:
                pass
            
            if database_odbc_driver and database_odbc_dbq:
                database_connection_properties['driver'] = database_odbc_driver.strip()
                database_connection_properties['dbq'] = database_odbc_dbq.strip()
        
        elif database_connection_type_case == 'PYODBCDRIVER':
            
            database_odbc_dbq = None
            database_odbc_driver = None
            database_odbc_server = None
            database_odbc_db = None
            database_odbc_trusted_conn = False
            database_odbc_uid = None
            database_odbc_pwd = None
            
            try:
                database_odbc_driver = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Driver>').strip()
            except AttributeError:
                pass
            
            try:
                database_odbc_dbq = os.path.abspath(self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><DBQ>').strip())
            except AttributeError:
                pass
            
            try:
                database_odbc_server = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Server>').strip()
            except AttributeError:
                pass
            
            try:
                database_odbc_db = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Database>').strip()
            except AttributeError:
                pass
            
            try:
                
                database_odbc_trusted_conn_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><TrustedConnection>').strip().upper()
                
                if database_odbc_trusted_conn_case == 'YES':
                    database_odbc_trusted_conn = True
                elif database_odbc_trusted_conn_case == 'NO':
                    database_odbc_trusted_conn = False
                elif database_odbc_trusted_conn_case == 'TRUE':
                    database_odbc_trusted_conn = True
                elif database_odbc_trusted_conn_case == 'FALSE':
                    database_odbc_trusted_conn = False
                elif database_odbc_trusted_conn_case == '1':
                    database_odbc_trusted_conn = True
                elif database_odbc_trusted_conn_case == '0':
                    database_odbc_trusted_conn = False
                else:
                    database_odbc_trusted_conn = False
                
            except AttributeError:
                pass
            
            try:
                database_odbc_uid = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Uid>')
            except AttributeError:
                pass
            
            try:
                database_odbc_pwd = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Pwd>')
            except AttributeError:
                pass
                        
            if database_odbc_driver and database_odbc_dbq:
                database_connection_properties['driver'] = database_odbc_driver.strip()
                database_connection_properties['dbq'] = database_odbc_dbq.strip()
                
            if database_odbc_driver and database_odbc_server and database_odbc_db:
                
                database_connection_properties['driver'] = database_odbc_driver.strip()
                database_connection_properties['server'] = database_odbc_server.strip()
                database_connection_properties['database'] = database_odbc_db.strip()
                
                database_connection_properties['trusted_connection'] = 'NO'
                if database_odbc_trusted_conn:
                    database_connection_properties['trusted_connection'] = 'YES'
                    
                if database_odbc_uid and database_odbc_pwd:
                    database_connection_properties['uid'] = database_odbc_uid
                    database_connection_properties['pwd'] = database_odbc_pwd
        
        elif database_connection_type_case == 'PYPYODBCDRIVER':
            
            database_odbc_dbq = None
            database_odbc_driver = None
            
            try:
                database_odbc_driver = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Driver>').strip()
            except TypeError:
                pass
            
            try:
                database_odbc_dbq = os.path.abspath(self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><DBQ>').strip())
            except TypeError:
                pass
            
            if database_odbc_driver and database_odbc_dbq:
                database_connection_properties['driver'] = database_odbc_driver.strip()
                database_connection_properties['dbq'] = database_odbc_dbq.strip()
        
        elif database_connection_type_case == 'SQLITE3DRIVER':
            
            database_filename = None
            
            try:
                database_filename = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><FileName>').strip()
            except TypeError:
                pass
            
            if database_filename:
                database_connection_properties['filename'] = database_filename.strip()
        
        elif database_connection_type_case == 'MYSQLDRIVER':
            
            database_host = None
            database_user = None
            database_passwd = None
            database_db = None
            
            try:
                database_host = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Host>').strip()
                database_user = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><User>').strip()
                database_passwd = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><Passwd>').strip()
                database_db = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><DatabaseConnection><DB>').strip()
            except TypeError:
                pass
            
            if database_host and database_user and database_passwd and database_db:
                database_connection_properties['host'] = database_host.strip()
                database_connection_properties['user'] = database_user.strip()
                database_connection_properties['passwd'] = database_passwd.strip()
                database_connection_properties['db'] = database_db.strip()

        return database_connection_properties
    
    def get_search_tool_name(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><SearchToolName>')
    
    def get_reset_database_tables(self):
        
        reset_database_tables = False
        
        reset_database_tables_case = None
        try:
            reset_database_tables_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><ResetDatabaseTables>').strip().upper()
        except TypeError:
            pass
            
        if reset_database_tables_case == 'YES':
            reset_database_tables = True
        elif reset_database_tables_case == 'NO':
            reset_database_tables = False
        elif reset_database_tables_case == 'TRUE':
            reset_database_tables = True
        elif reset_database_tables_case == 'FALSE':
            reset_database_tables = False
        elif reset_database_tables_case == '1':
            reset_database_tables = True
        elif reset_database_tables_case == '0':
            reset_database_tables = False
        else:
            reset_database_tables = False
            
        return reset_database_tables
    
    def get_update_investigator_ids(self):
        
        update_investigator_ids = False
        
        update_investigator_ids_case = None
        try:
            update_investigator_ids_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><UpdateInvestigatorIDs>').strip().upper()
        except TypeError:
            pass
            
        if update_investigator_ids_case == 'YES':
            update_investigator_ids = True
        elif update_investigator_ids_case == 'NO':
            update_investigator_ids = False
        elif update_investigator_ids_case == 'TRUE':
            update_investigator_ids = True
        elif update_investigator_ids_case == 'FALSE':
            update_investigator_ids = False
        elif update_investigator_ids_case == '1':
            update_investigator_ids = True
        elif update_investigator_ids_case == '0':
            update_investigator_ids = False
        else:
            update_investigator_ids = False
            
        return update_investigator_ids
    
    def get_update_publication_results(self):
        
        update_publication_results = False
        
        update_publication_results_case = None
        try:
            update_publication_results_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><UpdatePublicationResults>').strip().upper()
        except TypeError:
            pass
            
        if update_publication_results_case == 'YES':
            update_publication_results = True
        elif update_publication_results_case == 'NO':
            update_publication_results = False
        elif update_publication_results_case == 'TRUE':
            update_publication_results = True
        elif update_publication_results_case == 'FALSE':
            update_publication_results = False
        elif update_publication_results_case == '1':
            update_publication_results = True
        elif update_publication_results_case == '0':
            update_publication_results = False
        else:
            update_publication_results = False
            
        return update_publication_results
    
    def get_eutils_address(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><EUtilsAddress>')
    
    def get_email_address(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><EMailAddress>')
    
    def get_http_delay(self):
        return self.search_settings.get_integer_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><HTTPDelay>')
    
    def get_weekday_hours_start_time(self):
        
        return_timedelta = None
        
        start_time_hours = None
        start_time_minutes = None
        
        start_time_string = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><WeekdayHours><StartTime>')
        
        try:
            
            match = re.match('(\d+):(\d+)', start_time_string)
        
            if match:
                start_time_hours = int(match.group(1))
                start_time_minutes = int(match.group(2))
        
                return_timedelta = timedelta(hours=start_time_hours, minutes=start_time_minutes)
                
        except TypeError:
            pass
        
        return return_timedelta
    
    def get_weekday_hours_end_time(self):
        
        return_timedelta = None
        
        end_time_hours = None
        end_time_minutes = None
        
        end_time_string = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><WeekdayHours><EndTime>')
        
        try:
            
            match = re.match('(\d+):(\d+)', end_time_string)
            
            if match:
                end_time_hours = int(match.group(1))
                end_time_minutes = int(match.group(2))
        
            return_timedelta = timedelta(hours=end_time_hours, minutes=end_time_minutes)
            
        except TypeError:
            pass
        
        return return_timedelta
    
    def get_return_maximum(self):
        return self.search_settings.get_integer_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><ReturnMaximum>')
    
    def get_minimum_date(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><MinimumDate>')
    
    def get_maximum_date(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><MaximumDate>')
    
    def get_maximum_url_length(self):
        return self.search_settings.get_integer_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><MaximumURLLength>')
    
    def get_maximum_tries(self):
        return self.search_settings.get_integer_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><MaximumTries>')
    
    def get_eutils_use_history(self):
        
        eutils_use_history = False
        
        eutils_use_history_case = None
        try:
            eutils_use_history_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><EUtilsUseHistory>').strip().upper()
        except TypeError:
            pass
            
        if eutils_use_history_case == 'YES':
            eutils_use_history = True
        elif eutils_use_history_case == 'NO':
            eutils_use_history = False
        elif eutils_use_history_case == 'TRUE':
            eutils_use_history = True
        elif eutils_use_history_case == 'FALSE':
            eutils_use_history = False
        elif eutils_use_history_case == '1':
            eutils_use_history = True
        elif eutils_use_history_case == '0':
            eutils_use_history = False
        else:
            eutils_use_history = False
            
        return eutils_use_history
    
    def get_search_strategies(self):
    
        PERSON_BY_PERSON = 2**0
        PERSON_ORGANIZATION = 2**1
        PERSON_GRANT = 2**2
        PERSON_COAUTHOR = 2**3
        CTSA_GRANT = 2**4
        PMCID_CITE_BY_PMCID = 2**5
        PUBMED_ID_CITE_BY_PUBMED_ID = 2**6
        PUBMED_ID_NEIGHBOR_PUBMED_ID = 2**7
    
        search_strategies_array = [
                                   'PersonByPerson',
                                   'PersonOrganization',
                                   'PersonGrant',
                                   'PersonCoauthor',
                                   'CTSAGrant',
                                   'PMCIDCiteByPMCID',
                                   'PubMedIDCiteByPubMedID',
                                   'PubMedIDNeighborPubMedID']
            
        search_strategies = 0
        
        for strategy_index in range(0, len(search_strategies_array), 1):
            
            search_strategy_case = None
            try:
                search_strategy_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><SearchStrategies><' + search_strategies_array[strategy_index] + '>').strip().upper()
            except TypeError:
                pass
                
            if search_strategy_case == 'YES':
                search_strategies |= (2**strategy_index)
            elif search_strategy_case == 'NO':
                search_strategies &= ~(2**strategy_index)
            elif search_strategy_case == 'TRUE':
                search_strategies |= (2**strategy_index)
            elif search_strategy_case == 'FALSE':
                search_strategies &= ~(2**strategy_index)
            elif search_strategy_case == '1':
                search_strategies |= (2**strategy_index)
            elif search_strategy_case == '0':
                search_strategies &= ~(2**strategy_index)
            else:
                search_strategies &= ~(2**strategy_index)
                
        return search_strategies
    
    def get_timeout(self):
        return self.search_settings.get_integer_cdata('<BiblioAnalysisSettings><SearchPubMedSettings><Timeout>')
    
class ClassifyMeSHTermsSettings:
    '''
    classdocs
    '''
    
    settings_filename = os.path.abspath('pubmed_conf.xml')
    search_settings = None
    
    def __init__(self):
        '''
        Constructor
        '''
        
        self.read_settings()

    def read_settings(self):
        
        self.search_settings = TextXMLParser()
        
        file_in = open(self.settings_filename, 'rb')
        self.search_settings.ParseFile(file_in)
        file_in.close()
        
    def get_descriptors_file_name(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><ClassifyMeSHTermsSettings><MeSHDescriptorsFileName>')
        
    def get_qualifiers_file_name(self):
        return self.search_settings.get_string_cdata('<BiblioAnalysisSettings><ClassifyMeSHTermsSettings><MeSHQualifiersFileName>')
        
    def get_reset_database_tables(self):
        
        reset_database_tables = False
        
        reset_database_tables_case = None
        try:
            reset_database_tables_case = self.search_settings.get_string_cdata('<BiblioAnalysisSettings><ClassifyMeSHTermsSettings><ResetDatabaseTables>').strip().upper()
        except TypeError:
            pass
            
        if reset_database_tables_case == 'YES':
            reset_database_tables = True
        elif reset_database_tables_case == 'NO':
            reset_database_tables = False
        elif reset_database_tables_case == 'TRUE':
            reset_database_tables = True
        elif reset_database_tables_case == 'FALSE':
            reset_database_tables = False
        elif reset_database_tables_case == '1':
            reset_database_tables = True
        elif reset_database_tables_case == '0':
            reset_database_tables = False
        else:
            reset_database_tables = False
            
        return reset_database_tables
        
