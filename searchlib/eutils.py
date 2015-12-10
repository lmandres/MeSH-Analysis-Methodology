'''
Created on Jul 7, 2011

@author: Leo Andres (user)
'''

import http
import re
import socket
#import sys
import time
#import urllib
import urllib.error
import urllib.parse
import urllib.request
#import urllib2

class QueryEUtilsBase:

    http_error_codes = {
                        100: ('Continue', 'Request received, please continue'),
                        101: ('Switching Protocols', 'Switching to new protocol; obey Upgrade header'),
                        
                        200: ('OK', 'Request fulfilled, document follows'),
                        201: ('Created', 'Document created, URL follows'),
                        202: ('Accepted', 'Request accepted, processing continues off-line'),
                        203: ('Non-Authoritative Information', 'Request fulfilled from cache'),
                        204: ('No Content', 'Request fulfilled, nothing follows'),
                        205: ('Reset Content', 'Clear input form for further input.'),
                        206: ('Partial Content', 'Partial content follows.'),
                        
                        300: ('Multiple Choices', 'Object has several resources -- see URI list'),
                        301: ('Moved Permanently', 'Object moved permanently -- see URI list'),
                        302: ('Found', 'Object moved temporarily -- see URI list'),
                        303: ('See Other', 'Object moved -- see Method and URL list'),
                        304: ('Not Modified', 'Document has not changed since given time'),
                        305: ('Use Proxy', 'You must use proxy specified in Location to access this resource.'),
                        307: ('Temporary Redirect', 'Object moved temporarily -- see URI list'),
                        
                        400: ('Bad Request', 'Bad request syntax or unsupported method'),
                        401: ('Unauthorized', 'No permission -- see authorization schemes'),
                        402: ('Payment Required', 'No payment -- see charging schemes'),
                        403: ('Forbidden', 'Request forbidden -- authorization will not help'),
                        404: ('Not Found', 'Nothing matches the given URI'),
                        405: ('Method Not Allowed', 'Specified method is invalid for this server.'),
                        406: ('Not Acceptable', 'URI not available in preferred format.'),
                        407: ('Proxy Authentication Required', 'You must authenticate with this proxy before proceeding.'),
                        408: ('Request Timeout', 'Request timed out; try again later.'),
                        409: ('Conflict', 'Request conflict.'),
                        410: ('Gone', 'URI no longer exists and has been permanently removed.'),
                        411: ('Length Required', 'Client must specify Content-Length.'),
                        412: ('Precondition Failed', 'Precondition in headers is false.'),
                        413: ('Request Entity Too Large', 'Entity is too large.'),
                        414: ('Request-URI Too Long', 'URI is too long.'),
                        415: ('Unsupported Media Type', 'Entity body in unsupported format.'),
                        416: ('Requested Range Not Satisfiable', 'Cannot satisfy request range.'),
                        417: ('Expectation Failed', 'Expect condition could not be satisfied.'),
                        
                        500: ('Internal Server Error', 'Server got itself in trouble'),
                        501: ('Not Implemented', 'Server does not support this operation'),
                        502: ('Bad Gateway', 'Invalid responses from another server/proxy.'),
                        503: ('Service Unavailable', 'The server cannot process the request due to a high load'),
                        504: ('Gateway Timeout', 'The gateway server did not receive a timely response'),
                        505: ('HTTP Version Not Supported', 'Cannot fulfill request.')}
    
    base_eutils_url = None
    
    maximum_tries = None
    sleep_delay = None
    timeout = None
    maximum_url_length = None
    
    def __init__(self, eutils_url_in):
        self.base_eutils_url = eutils_url_in
        
    def set_base_eutils_url(self, eutils_url_in):
        self.base_eutils_url = eutils_url_in
        
    def get_base_eutils_url(self):
        return self.base_eutils_url
    
    def set_maximum_tries(self, maximum_tries_in):
        self.maximum_tries = int(maximum_tries_in)
        
    def get_maximum_tries(self):
        return self.maximum_tries
    
    def set_sleep_delay(self, sleep_delay_in):
        self.sleep_delay = int(sleep_delay_in)
        
    def get_sleep_delay(self):
        return self.sleep_delay
    
    def set_timeout(self, timeout_in):
        self.timeout = int(timeout_in)
        
    def get_timeout(self):
        return self.timeout
    
    def set_maximum_url_length(self, maximum_url_length_in):
        self.maximum_url_length = int(maximum_url_length_in)
        
    def get_maximum_url_lengt(self):
        return self.maximum_url_length
    
    def run_eutils_request(self, eutils_variables_in):
        
        attempt_number = 0
        eutils_request_variables = {}
        
        for dict_key in eutils_variables_in:
            if eutils_variables_in[dict_key] != None:
                eutils_request_variables[dict_key] = eutils_variables_in[dict_key]
            
        print('\nDoing EUtilities request at ' + time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime()) + '\n' + self.base_eutils_url + '?' + urllib.parse.urlencode(eutils_request_variables) + '\n')
                   
        while True:
            
            xml_string = None
            time.sleep(self.sleep_delay)
            
            try:
                
                response = None
                
                if self.maximum_url_length != None and self.maximum_url_length <= 1600:
                    response = urllib.request.urlopen(url = self.base_eutils_url + '?' + urllib.parse.urlencode(eutils_request_variables), timeout = self.timeout)
                else:
                    response = urllib.request.urlopen(url = self.base_eutils_url, data = urllib.parse.urlencode(eutils_request_variables).encode('utf-8'), timeout = self.timeout)
                    
                xml_string = response.read()

            except OSError as ose:
                if str(ose).strip() == '[Errno 11004] getaddrinfo failed':
                    print('Network connection unavailable.')
                    attempt_number -= 1
                else:
                    print('OSError')
                    print(ose)
            except socket.error as se:
                # ADD SOMETHING HERE TO DESCRIBE THE ERROR BETTER
                print('socket.error')
                print(se)
            except urllib.error.HTTPError as er:
                print(str(er.code) + ": " + self.http_error_codes[er.code][1])
            except urllib.error.URLError as er:
                print(er)
            except socket.timeout:
                print('Request timed out.')
            except http.client.BadStatusLine as bsl:
                # ADD SOMETHING HERE TO DESCRIBE THE ERROR BETTER
                print('Bad status line (?).')
                print(bsl)
                
            if xml_string != None:
                break
            
            attempt_number += 1
            print('Search result invalid.  Attempt ' + str(attempt_number) + '.')
        
            if self.maximum_tries < attempt_number:
                print('Maximum tries exceeded.')
                break
            
        return xml_string
    
class IteratePubMedESearchResults(QueryEUtilsBase):

    result_count = 0
    result_return_maximum = 0
    result_return_start = 0
    
    result_idlist_iter = None

    eutils_esearch_variables = None
    
    def __init__(self, esearch_settings_in):
            
        self.result_count = 0
        self.result_return_maximum = 0
        self.result_return_start = 0
        
        self.eutils_esearch_variables = {
                                         'rettype' : 'uilist',
                                         'retstart' : 0,
                                         'retmax' : None,
                                         'db' : 'pubmed',
                                         'usehistory' : None,
                                         'term' : None,
                                         'email' : None,
                                         'tool' : None,
                                         'query_key' : None,
                                         'WebEnv' : None}
        
        for dict_key in self.eutils_esearch_variables:
            try:
                if self.eutils_esearch_variables[dict_key] == None and esearch_settings_in[dict_key] != None:
                    self.eutils_esearch_variables[dict_key] = esearch_settings_in[dict_key]
            except KeyError:
                pass
                
        self.eutils_esearch_variables['query_key'] = None
        self.eutils_esearch_variables['retstart'] = 0

        self.set_base_eutils_url(esearch_settings_in['base_address'] + '/esearch.fcgi')
        self.set_sleep_delay(esearch_settings_in['sleep_delay'])
        self.set_maximum_tries(esearch_settings_in['maximum_tries'])
        self.set_timeout(esearch_settings_in['timeout'])
            
        self.result_idlist_iter = None
        self.__run_eutils_esearch_request()
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        try:
            return self.result_idlist_iter.__next__().group(1).decode('utf-8')
        except StopIteration:

            if self.result_count <= (self.result_return_maximum + self.result_return_start):
                raise StopIteration
            else:
                
                self.eutils_esearch_variables['retstart'] = self.eutils_esearch_variables['retstart'] + self.eutils_esearch_variables['retmax']
                
                if self.result_count <= self.eutils_esearch_variables['retstart'] + self.eutils_esearch_variables['retmax']:
                    print('\nRetrieving Articles ' + str(self.eutils_esearch_variables['retstart'] + 1) + ' to ' + str(self.result_count) + '.')
                else:
                    print('\nRetrieving Articles ' + str(self.eutils_esearch_variables['retstart'] + 1) + ' to ' + str(self.eutils_esearch_variables['retstart'] + self.eutils_esearch_variables['retmax']) + '.')
        
                self.result_idlist_iter = None
                self.__run_eutils_esearch_request()
        
                try:
                    return self.result_idlist_iter.__next__().group(1).decode('utf-8')
                except StopIteration:
                    raise StopIteration
        
    def get_query_key(self):
        return self.eutils_esearch_variables['query_key']
    
    def get_web_env(self):
        return self.eutils_esearch_variables['WebEnv']
    
    def get_result_count(self):
        return self.result_count
    
    def __run_eutils_esearch_request(self):
        
        while True:
            
            xml_string = self.run_eutils_request(self.eutils_esearch_variables)
            
            match = re.search(b'<Count>(\d+)</Count>.*?<RetMax>(\d+)</RetMax>.*?<RetStart>(\d+)</RetStart>.*?(<IdList>.*?</IdList>)', xml_string, re.DOTALL)
            if match:
                break
            
            match = re.search(b'<Count>(\d+)</Count>.*?<RetMax>(\d+)</RetMax>.*?<RetStart>(\d+)</RetStart>.*?(<IdList/>)', xml_string, re.DOTALL)
            if match:
                break
            
        self.result_count = int(match.group(1))
        self.result_return_maximum = int(match.group(2))
        self.result_return_start = int(match.group(3))

        self.result_idlist_iter = re.finditer(b'<Id>(\d+)</Id>', match.group(4), re.DOTALL)
        
        try:
            self.eutils_esearch_variables['query_key'] = None
        except KeyError:
            pass
        
        try:
            self.eutils_esearch_variables['WebEnv'] = None
        except KeyError:
            pass
        
        try:
            if self.eutils_esearch_variables['usehistory'] == 'y':
                match = re.search(b'<Count>\d+</Count>.*?<RetMax>\d+</RetMax>.*?<RetStart>\d+</RetStart>.*?<QueryKey>(.*?)</QueryKey>.*?<WebEnv>(.*?)</WebEnv>.*?<IdList>', xml_string, re.DOTALL)
                if match:
                    self.eutils_esearch_variables['query_key'] = match.group(1).strip()
                    self.eutils_esearch_variables['WebEnv'] = match.group(2).strip()
                    self.eutils_esearch_variables['term'] = None
                else:
                    self.eutils_esearch_variables['usehistory'] = None
                    self.eutils_esearch_variables['query_key'] = None
                    self.eutils_esearch_variables['WebEnv'] = None
                    
        except KeyError:
            pass

class IteratePubMedEFetchData(QueryEUtilsBase):

    result_count = 0
    result_return_maximum = 0
    result_return_start = 0
    
    efetch_pubmed_data_iter = None
    
    efetch_last_pubmed_id = None
    efetch_pubmed_id_iter = None
    
    eutils_efetch_variables = {
                               'retmode' : 'xml',
                               'retstart' : None,
                               'retmax' : None,
                               'db' : 'pubmed',
                               'usehistory' : None,
                               'email' : None,
                               'tool' : None,
                               'query_key' : None,
                               'WebEnv' : None}
    
    def __init__(self, efetch_settings_in, efetch_pubmed_id_iterable_in=None):
        
        for dict_key in self.eutils_efetch_variables:
            try:
                self.eutils_efetch_variables[dict_key] = efetch_settings_in[dict_key]
            except KeyError:
                pass
        
        if efetch_pubmed_id_iterable_in == None and self.eutils_efetch_variables['query_key'] != None:
            self.eutils_efetch_variables['retstart'] = 0
        else:
            
            try:
                self.efetch_pubmed_id_iter = efetch_pubmed_id_iterable_in.__iter__()
            except AttributeError:
                self.efetch_pubmed_id_iter = [].__iter__()
            
            self.eutils_efetch_variables['query_key'] = None
            self.eutils_efetch_variables['retstart'] = None
            self.eutils_efetch_variables['retmax'] = None

        self.set_base_eutils_url(efetch_settings_in['base_address'] + '/efetch.fcgi')
        
        try:
            self.set_sleep_delay(int(efetch_settings_in['sleep_delay']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_tries(int(efetch_settings_in['maximum_tries']))
        except TypeError:
            pass
        
        try:
            self.set_timeout(int(efetch_settings_in['timeout']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_url_length(int(efetch_settings_in['maximum_url_length']))
        except TypeError:
            pass
        
        try:
            self.result_return_maximum = int(efetch_settings_in['retmax'])
        except TypeError:
            pass
        
        try:
            self.result_count = int(efetch_settings_in['result_count'])
        except TypeError:
            pass
        
        self.efetch_pubmed_data_iter = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        if self.eutils_efetch_variables['query_key'] != None:
            return self.next_by_query_key()
        else:
            return self.next_by_id_list()
            
    
    def next_by_query_key(self):
        
        try:
            return self.efetch_pubmed_data_iter.__next__().group(1)
        except StopIteration:
            self.eutils_efetch_variables['retstart'] = self.eutils_efetch_variables['retstart'] + self.eutils_efetch_variables['retmax']
        except AttributeError:
            pass
        
        if self.eutils_efetch_variables['retstart'] >= self.result_count:
            self.efetch_pubmed_data_iter = [].__iter__()
        else:
            
            if self.result_count <= self.eutils_efetch_variables['retstart'] + self.eutils_efetch_variables['retmax']:
                print('\nRetrieving Articles ' + str(self.eutils_efetch_variables['retstart'] + 1) + ' to ' + str(self.result_count) + '.')
            else:
                print('\nRetrieving Articles ' + str(self.eutils_efetch_variables['retstart'] + 1) + ' to ' + str(self.eutils_efetch_variables['retstart'] + self.eutils_efetch_variables['retmax']) + '.')
                
                
            self.efetch_pubmed_data_iter = None
            self.__run_query_key_eutils_efetch_request()
        
        try:
            return self.efetch_pubmed_data_iter.__next__().group(1)
        except StopIteration:
            raise StopIteration
    
    def next_by_id_list(self):
        
        try:
            return self.efetch_pubmed_data_iter.__next__().group(1)
        except StopIteration:
            if self.maximum_url_length == None:
                raise StopIteration
        except AttributeError:
            pass
                
        self.efetch_pubmed_data_iter = None
        self.__run_id_list_eutils_efetch_request()
        
        try:
            return self.efetch_pubmed_data_iter.__next__().group(1)
        except StopIteration:
            raise StopIteration
        
    def __run_query_key_eutils_efetch_request(self):
        
        xml_string = None
        efetch_post_variables = {}
        
        for dict_key in self.eutils_efetch_variables:
            if self.eutils_efetch_variables[dict_key] != None:
                efetch_post_variables[dict_key] = self.eutils_efetch_variables[dict_key]
                            
        while True:
            
            xml_string = self.run_eutils_request(efetch_post_variables)
            if xml_string != None:
                break
            
        self.efetch_pubmed_data_iter = re.finditer(b'(<PubmedArticle>.*?</PubmedArticle>)', xml_string, re.DOTALL)
                        
    def __run_id_list_eutils_efetch_request(self):
        
        xml_string = None
        
        efetch_post_variables = {}
        efetch_pubmed_id_list = []
        efetch_post_data = None
        
        for dict_key in self.eutils_efetch_variables:
            if self.eutils_efetch_variables[dict_key] != None:
                efetch_post_variables[dict_key] = self.eutils_efetch_variables[dict_key]
            
        if self.maximum_url_length == None:
            
            for list_item in self.efetch_pubmed_id_iter:
                efetch_pubmed_id_list.append(list_item)
                
        else:
        
            if self.efetch_last_pubmed_id != None:
                efetch_pubmed_id_list.append(self.efetch_last_pubmed_id)
            
            while True:
                
                efetch_post_variables['id'] =  ','.join([str(list_item).strip() for list_item in efetch_pubmed_id_list])
                
                self.efetch_last_pubmed_id = None
                try:
                    self.efetch_last_pubmed_id = self.efetch_pubmed_id_iter.__next__()
                    efetch_post_variables['id'] += ',' + str(self.efetch_last_pubmed_id).strip()
                except StopIteration:
                    pass
                
                if self.maximum_url_length <= 1600:
                    efetch_post_data = self.get_base_eutils_url() + '?' + urllib.parse.urlencode(efetch_post_variables)
                else:
                    efetch_post_data = urllib.parse.urlencode(efetch_post_variables)
                
                if len(efetch_post_data) <= self.maximum_url_length:
                    if self.efetch_last_pubmed_id != None:
                        efetch_pubmed_id_list.append(self.efetch_last_pubmed_id)
                    else:
                        break
                else:
                    break
        
        if len(efetch_pubmed_id_list) <= 0:
            self.efetch_pubmed_data_iter = [].__iter__()
        else:
            
            efetch_post_variables['id'] =  ','.join([str(list_item).strip() for list_item in efetch_pubmed_id_list])
                            
            while True:
                
                xml_string = self.run_eutils_request(efetch_post_variables)
                if xml_string != None:
                    break
                
            self.efetch_pubmed_data_iter = re.finditer(b'(<PubmedArticle>.*?</PubmedArticle>)', xml_string, re.DOTALL)

class IteratePubMedCentralELinkCitedByPMCIDs(QueryEUtilsBase):
    
    elink_pmcid_iter = None
    
    base_address = None
    sleep_delay = None
    maximum_tries = None
    timeout = None
    
    eutils_elink_variables = {
                               'retmode' : 'xml',
                               'dbfrom' : 'pmc',
                               'db' : 'pmc',
                               'id' : None,
                               'email' : None,
                               'tool' : None}
    
    def __init__(self, elink_settings_in, elink_pmcids_in):
        
        for dict_key in self.eutils_elink_variables:
            try:
                self.eutils_elink_variables[dict_key] = elink_settings_in[dict_key]
            except KeyError:
                pass
            
        try:
            self.eutils_elink_variables['usehistory'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['query_key'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['WebEnv'] = None
        except KeyError:
            pass

        self.set_base_eutils_url(elink_settings_in['base_address'] + '/elink.fcgi')
        self.eutils_elink_variables['id'] = elink_pmcids_in
        self.eutils_elink_variables['db'] = 'pmc'
        
        try:
            self.set_sleep_delay(int(elink_settings_in['sleep_delay']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_tries(int(elink_settings_in['maximum_tries']))
        except TypeError:
            pass
        
        try:
            self.set_timeout(int(elink_settings_in['timeout']))
        except TypeError:
            pass
        
        self.elink_pmcid_iter = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        try:
            return self.elink_pmcid_iter.__next__().group(1).decode('utf-8')
        except StopIteration:
            raise StopIteration
        except AttributeError:
            
            try:
                self.__run_elink_request()
                return self.elink_pmcid_iter.__next__().group(1).decode('utf-8')
            except AttributeError:
                raise StopIteration
            
    def __run_elink_request(self):
        
        xml_string = None
        match = None
            
        self.elink_pmcid_iter = None
    
        while True:
            
            xml_string = self.run_eutils_request(self.eutils_elink_variables)
            if xml_string != None:
                break
            
        match = re.search(b'.*(<LinkSetDb>.*?<DbTo>pmc</DbTo>.*?<LinkName>pmc_pmc_citedby</LinkName>.*?</LinkSetDb>)', xml_string, re.DOTALL)
        if match:
            self.elink_pmcid_iter = re.finditer(b'<Id>(.*?)</Id>', match.group(1), re.DOTALL)

class IteratePubMedIDELinkCitedByPubMedIDs(QueryEUtilsBase):
    
    elink_pmcid_iter = None
    
    base_address = None
    sleep_delay = None
    maximum_tries = None
    timeout = None
    
    eutils_elink_variables = {
                               'retmode' : 'xml',
                               'dbfrom' : 'pubmed',
                               'db' : 'pubmed',
                               'id' : None,
                               'email' : None,
                               'tool' : None}
    
    def __init__(self, elink_settings_in, elink_pubmed_ids_in):
        
        for dict_key in self.eutils_elink_variables:
            try:
                self.eutils_elink_variables[dict_key] = elink_settings_in[dict_key]
            except KeyError:
                pass
            
        try:
            self.eutils_elink_variables['usehistory'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['query_key'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['WebEnv'] = None
        except KeyError:
            pass

        self.set_base_eutils_url(elink_settings_in['base_address'] + '/elink.fcgi')
        self.eutils_elink_variables['id'] = elink_pubmed_ids_in
        self.eutils_elink_variables['db'] = 'pubmed'
        
        try:
            self.set_sleep_delay(int(elink_settings_in['sleep_delay']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_tries(int(elink_settings_in['maximum_tries']))
        except TypeError:
            pass
        
        try:
            self.set_timeout(int(elink_settings_in['timeout']))
        except TypeError:
            pass
        
        self.elink_pmcid_iter = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        try:
            return self.elink_pmcid_iter.__next__().group(1).decode('utf-8')
        except StopIteration:
            raise StopIteration
        except AttributeError:
            
            try:
                self.__run_elink_request()
                return self.elink_pmcid_iter.__next__().group(1).decode('utf-8')
            except AttributeError:
                raise StopIteration
            
    def __run_elink_request(self):
        
        xml_string = None
        match = None
            
        self.elink_pmcid_iter = None
    
        while True:
            
            xml_string = self.run_eutils_request(self.eutils_elink_variables)
            if xml_string != None:
                break
            
        match = re.search(b'.*(<LinkSetDb>.*?<DbTo>pubmed</DbTo>.*?<LinkName>pubmed_pubmed_citedin</LinkName>.*?</LinkSetDb>)', xml_string, re.DOTALL)
        if match:
            self.elink_pmcid_iter = re.finditer(b'<Id>(.*?)</Id>', match.group(1), re.DOTALL)

class IteratePubMedCentralELinkToPubMedIDs(QueryEUtilsBase):
    
    elink_pmcid_iter = None
    elink_pubmed_id_iter = None
    
    elink_last_pubmed_id = None
    
    base_address = None
    sleep_delay = None
    maximum_tries = None
    timeout = None
    
    eutils_elink_variables = {
                               'retmode' : 'xml',
                               'dbfrom' : 'pmc',
                               'db' : 'pubmed',
                               'id' : None,
                               'email' : None,
                               'tool' : None}
    
    def __init__(self, elink_settings_in, elink_pmcid_iter_in):
        
        for dict_key in self.eutils_elink_variables:
            try:
                self.eutils_elink_variables[dict_key] = elink_settings_in[dict_key]
            except KeyError:
                pass
            
        try:
            self.eutils_elink_variables['usehistory'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['query_key'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['WebEnv'] = None
        except KeyError:
            pass

        self.set_base_eutils_url(elink_settings_in['base_address'] + '/elink.fcgi')
        self.elink_pmcid_iter = elink_pmcid_iter_in
        
        self.eutils_elink_variables['db'] = 'pubmed'
        
        try:
            self.set_sleep_delay(int(elink_settings_in['sleep_delay']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_tries(int(elink_settings_in['maximum_tries']))
        except TypeError:
            pass
        
        try:
            self.set_timeout(int(elink_settings_in['timeout']))
        except TypeError:
            pass
    
        self.elink_pubmed_id_iter = None
        
    def __iter__(self):
        return self
    
    def __next__(self):
        
        try:
            return self.elink_pubmed_id_iter.__next__().group(1).decode('utf-8')
        except StopIteration:
            raise StopIteration
        except AttributeError:
        
            xml_string = None
            match = None
        
            elink_post_variables = {}
            elink_pmcid_list = []
            elink_post_data = None
        
            for dict_key in self.eutils_elink_variables:
                if self.eutils_elink_variables[dict_key] != None:
                    elink_post_variables[dict_key] = self.eutils_elink_variables[dict_key]
            
            if self.maximum_url_length == None:
                
                for list_item in self.elink_pmcid_iter:
                    elink_pmcid_list.append(list_item)
                    
            else:
            
                if self.efetch_last_pubmed_id != None:
                    elink_pmcid_list.append(self.elink_last_pubmed_id)
            
                while True:
                    
                    elink_post_variables['id'] =  ','.join([str(list_item).strip() for list_item in elink_pmcid_list])
                    
                    self.elink_last_pubmed_id = None
                    try:
                        self.elink_last_pubmed_id = self.elink_pmcid_iter.__next__()
                        elink_post_variables['id'] += ',' + str(self.elink_last_pubmed_id).strip()
                    except StopIteration:
                        pass
                    
                    if self.maximum_url_length <= 1600:
                        elink_post_data = self.get_base_eutils_url() + '?' + urllib.parse.urlencode(elink_post_variables)
                    else:
                        elink_post_data = urllib.parse.urlencode(elink_post_variables)
                    
                    if len(elink_post_data) <= self.maximum_url_length:
                        if self.elink_last_pubmed_id != None:
                            elink_pmcid_list.append(self.elink_last_pubmed_id)
                        else:
                            break
                    else:
                        break
        
            if len(elink_pmcid_list) <= 0:
                raise StopIteration
            else:
                    
                self.elink_pubmed_id_iter = None
                self.eutils_elink_variables['id'] = ','.join([str(list_item).strip() for list_item in elink_pmcid_list])
            
                while True:
                    
                    xml_string = self.run_eutils_request(self.eutils_elink_variables)
                    if xml_string != None:
                        break
                    
                match = re.search(b'.*(<LinkSetDb>.*?<DbTo>pubmed</DbTo>.*?<LinkName>pmc_pubmed</LinkName>.*?</LinkSetDb>)', xml_string, re.DOTALL)
                if match:
                    self.elink_pubmed_id_iter = re.finditer(b'<Id>(.*?)</Id>', match.group(1), re.DOTALL)
            
                try:
                    return self.elink_pubmed_id_iter.__next__().group(1).decode('utf-8')
                except AttributeError:
                    raise StopIteration

class IteratePubMedIDELinkNeighborPubMedIDs(QueryEUtilsBase):
    
    base_address = None
    sleep_delay = None
    maximum_tries = None
    timeout = None
    
    eutils_elink_variables = {
                               'retmode' : 'xml',
                               'dbfrom' : 'pubmed',
                               'db' : 'pubmed',
                               'id' : None,
                               'cmd' : 'neighbor_score',
                               'email' : None,
                               'tool' : None}
    
    def __init__(self, elink_settings_in, elink_pubmed_ids_in):
        
        for dict_key in self.eutils_elink_variables:
            try:
                self.eutils_elink_variables[dict_key] = elink_settings_in[dict_key]
            except KeyError:
                pass
            
        try:
            self.eutils_elink_variables['usehistory'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['query_key'] = None
        except KeyError:
            pass
            
        try:
            self.eutils_elink_variables['WebEnv'] = None
        except KeyError:
            pass

        self.set_base_eutils_url(elink_settings_in['base_address'] + '/elink.fcgi')
        self.eutils_elink_variables['id'] = elink_pubmed_ids_in
        self.eutils_elink_variables['db'] = 'pubmed'
        
        try:
            self.set_sleep_delay(int(elink_settings_in['sleep_delay']))
        except TypeError:
            pass
        
        try:
            self.set_maximum_tries(int(elink_settings_in['maximum_tries']))
        except TypeError:
            pass
        
        try:
            self.set_timeout(int(elink_settings_in['timeout']))
        except TypeError:
            pass
        
        self.elink_pmcid_iter = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        next_item = None
        
        try:
            next_item = self.elink_pmcid_iter.__next__()
        except StopIteration:
            raise StopIteration
        except AttributeError:
            
            try:
                self.__run_elink_request()
                next_item = self.elink_pmcid_iter.__next__()
            except AttributeError:
                raise StopIteration
            
        if next_item:
            
            match = re.search(b'<Id>(.*?)</Id>.*?<Score>(.*?)</Score>', next_item.group(1), re.DOTALL)
            if match:
                return (match.group(1).decode('utf-8'), int(match.group(2).decode('utf-8')))
            
        raise StopIteration
            
    def __run_elink_request(self):
        
        xml_string = None
        match = None
            
        self.elink_pmcid_iter = None
    
        while True:
            
            xml_string = self.run_eutils_request(self.eutils_elink_variables)
            if xml_string != None:
                break
            
        match = re.search(b'.*(<LinkSetDb>.*?<DbTo>pubmed</DbTo>.*?<LinkName>pubmed_pubmed</LinkName>.*?</LinkSetDb>)', xml_string, re.DOTALL)
        if match:
            self.elink_pmcid_iter = re.finditer(b'<Link>(.*?)</Link>', match.group(1), re.DOTALL)
        
class EUtilsPubMed:
    '''
    classdocs
    '''

    eutils_settings = {
                       'base_address' : 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils',
                       'sleep_delay' : 1,
                       'maximum_tries' : 3,
                       'timeout' : 60,
                       'maximum_url_length' : None,
                       'retmax' : 100000,
                       'usehistory' : 'y',
                       'term' : None,
                       'email' : None,
                       'tool' : None,
                       'query_key' : None,
                       'WebEnv' : None,
                       'result_count' : None}
         
    def __init__(self):
        '''
        Constructor
        '''
    
    def set_eutils_address(self, address_in):
        self.eutils_settings['base_address'] = str(address_in).strip().rstrip('/')
        
    def get_eutils_address(self):
        return self.eutils_settings['base_address']
    
    def set_sleep_delay(self, delay_in):
        self.eutils_settings['sleep_delay'] = int(delay_in)
        
    def get_sleep_delay(self):
        return self.eutils_settings['sleep_delay']
    
    def set_maximum_tries(self, tries_in):
        self.eutils_settings['maximum_tries'] = int(tries_in)
        
    def get_maximum_tries(self):
        return self.eutils_settings['maximum_tries']
    
    def set_timeout(self, timeout_in):
        self.eutils_settings['timeout'] = int(timeout_in)
        
    def get_timeout(self):
        return self.eutils_settings['timeout']
    
    def set_maximum_url_length(self, length_in):
        self.eutils_settings['maximum_url_length'] = int(length_in)
        
    def get_maximum_url_length(self):
        return self.eutils_settings['maximum_url_length']
    
    def set_return_maximum(self, maximum_in):
        self.eutils_settings['retmax'] = int(maximum_in)
        
    def get_return_maximum(self):
        return self.eutils_settings['retmax']
        
    def get_eutils_database(self):
        return self.eutils_settings['db']
    
    def set_eutils_use_history(self, history_in):
        
        if history_in:
            self.eutils_settings['usehistory'] = 'y'
        else:
            try:
                del(self.eutils_settings['usehistory'])
            except KeyError:
                pass
            
    def get_eutils_use_history(self):
        
        try:
            return self.eutils_settings['usehistory']
        except KeyError:
            return None
        
    def set_email_address(self, email_in):
        self.eutils_settings['email'] = email_in
        
    def get_email_address(self):
        return self.eutils_settings['email']
    
    def set_tool_name(self, name_in):
        self.eutils_settings['tool'] = name_in
        
    def get_tool_name(self):
        return self.eutils_settings['tool']
    
    def pubmed_esearch_id_iter(self, esearch_term_in):
                        
        self.eutils_settings['term'] = esearch_term_in
        pubmed_esearch_results = IteratePubMedESearchResults(self.eutils_settings)
        
        self.eutils_settings['query_key'] = pubmed_esearch_results.get_query_key()
        self.eutils_settings['WebEnv'] = pubmed_esearch_results.get_web_env()
        self.eutils_settings['result_count'] = pubmed_esearch_results.get_result_count()
        
        return pubmed_esearch_results
    
    def pubmed_efetch_data_iter(self, efetch_pubmed_id_iterable_in):
        return IteratePubMedEFetchData(self.eutils_settings, efetch_pubmed_id_iterable_in)
                
    def pubmed_esearch_data_iter(self, esearch_term_in):
        
        self.eutils_settings['WebEnv'] = None
        self.eutils_settings['query_key'] = None
        self.eutils_settings['result_count'] = None
            
        self.eutils_settings['term'] = esearch_term_in
        pubmed_esearch_results = IteratePubMedESearchResults(self.eutils_settings)
                        
        try:
            
            if self.eutils_settings['usehistory'] == 'y':
                   
                self.eutils_settings['WebEnv'] = pubmed_esearch_results.get_web_env()
                self.eutils_settings['query_key'] = pubmed_esearch_results.get_query_key()
                self.eutils_settings['result_count'] = pubmed_esearch_results.get_result_count()
                
                return IteratePubMedEFetchData(self.eutils_settings)
            
        except KeyError:
            return IteratePubMedEFetchData(self.eutils_settings, pubmed_esearch_results)
    
    def elink_pmcid_cited_by_pmcids(self, elink_pmcids_in):
        
        return_iter = []
        
        try:
            return_iter = IteratePubMedCentralELinkCitedByPMCIDs(self.eutils_settings, elink_pmcids_in.strip())
        except AttributeError:
            pass
            
        return return_iter
    
    def elink_pmcids_link_to_pubmed_ids(self, pmcid_iter_in):
        
        return_iter = []
        
        try:
            return_iter = IteratePubMedCentralELinkToPubMedIDs(self.eutils_settings, pmcid_iter_in)
        except AttributeError:
            pass
            
        return return_iter
    
    def elink_pubmed_id_cited_by_pubmed_ids(self, elink_pubmed_ids_in):
        
        return_iter = []
        
        try:
            return_iter = IteratePubMedIDELinkCitedByPubMedIDs(self.eutils_settings, elink_pubmed_ids_in.strip())
        except AttributeError:
            pass
            
        return return_iter
    
    def elink_pubmed_id_neighbor_pubmed_ids(self, elink_pubmed_ids_in):
        
        return_iter = []
        
        try:
            return_iter = IteratePubMedIDELinkNeighborPubMedIDs(self.eutils_settings, elink_pubmed_ids_in.strip())
        except AttributeError:
            pass
            
        return return_iter
