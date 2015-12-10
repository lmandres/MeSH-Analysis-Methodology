from java.sql import DriverManager
from java.sql import SQLException

import sun.jdbc.odbc.JdbcOdbcDriver

class PubMedAccessJDBCODBCDatabaseController:
    '''
    classdocs
    '''
    
    database_connection = None
    
    def open_database(self, odbc_connection_string_in):
        
        DriverManager.registerDriver(sun.jdbc.odbc.JdbcOdbcDriver())
        
        try:
            self.database_connection = DriverManager.getConnection('jdbc:odbc:' + odbc_connection_string_in)
            return True
        except TypeError:
            self.database_connection = None
            return False
        except SQLException, sqlex:
            
            if sqlex.getErrorCode() == 63:
                self.database_connection = None
                return False
            else:
                raise sqlex
        
    def close_database(self):
        
        try:
            
            self.database_connection.close()
            self.database_connection = None
            return True
        
        except AttributeError:
            
            self.database_connection = None
            return False
    
    def run_sql_query(self, sql_query_in):
        return self.database_connection.createStatement().executeQuery(sql_query_in)
    
class PubMedAccessJDBCODBCDatabaseSearch(PubMedAccessJDBCODBCDatabaseController):
    
    PERSON_BY_PERSON = 2**0
    PERSON_ORGANIZATION = 2**1
    PERSON_GRANT = 2**2
    PERSON_COAUTHOR = 2**3
    
    search_method_query_parts = {
                                 PERSON_BY_PERSON : {
                                                      'selectFields' : """
                                                                          sq_1.PERSON_ID AS Search_Person_ID,
                                                                          Iif(sq_1.FAMILY_NAME Is Not Null, Trim(sq_1.FAMILY_NAME), '') AS Search_Person_Last_Name,
                                                                          Iif(sq_1.FIRST_GIVEN_NAME Is Not Null, Trim(sq_1.FIRST_GIVEN_NAME), '') AS Search_Person_First_Name,
                                                                          Iif(sq_1.SECOND_GIVEN_NAME Is Not Null, Left(sq_1.SECOND_GIVEN_NAME, 1), '') AS Search_Person_Middle_Name,
                                                                          sq_2.PERSON_ID AS Co_Investigator_ID,
                                                                          Iif(sq_2.FAMILY_NAME Is Not Null, Trim(sq_2.FAMILY_NAME), '') AS Co_Investigator_Last_Name,
                                                                          Iif(sq_2.FIRST_GIVEN_NAME Is Not Null, Trim(sq_2.FIRST_GIVEN_NAME), '') AS Co_Investigator_First_Name,
                                                                          Iif(sq_2.SECOND_GIVEN_NAME Is Not Null, Left(sq_2.SECOND_GIVEN_NAME, 1), '') AS Co_Investigator_Middle_Name""",
                                                       'fromClause' : """
                                                                         FROM
                                                                             (
                                                                                 SELECT DISTINCT
                                                                                     ari_1_1.PERSON_ID,
                                                                                     ari_1_1.FAMILY_NAME,
                                                                                     ari_1_1.FIRST_GIVEN_NAME,
                                                                                     ari_1_1.SECOND_GIVEN_NAME
                                                                                 FROM (
                                                                                        SELECT DISTINCT RAPIDAPP_PERSON_ROSTER.PERSON_ID, RAPIDAPP_PERSON_ROSTER.FAMILY_NAME, RAPIDAPP_PERSON_ROSTER.FIRST_GIVEN_NAME, RAPIDAPP_PERSON_ROSTER.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN RAPIDAPP_PERSON_ROSTER ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = RAPIDAPP_PERSON_ROSTER.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                     ) ari_1_1
                                                                                 UNION
                                                                                 SELECT DISTINCT
                                                                                     art_1_1.PERSON_ID,
                                                                                     art_1_1.FAMILY_NAME,
                                                                                     art_1_1.FIRST_GIVEN_NAME,
                                                                                     art_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((RAPIDAPP_PERSON_ROSTER AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN RAPIDAPP_PERSON_ROSTER AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) art_1_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ia_1_1.PERSON_ID,
                                                                                    ia_1_1.FAMILY_NAME,
                                                                                    ia_1_1.FIRST_GIVEN_NAME,
                                                                                    ia_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT tbl_Person_Aliases.PERSON_ID, tbl_Person_Aliases.FAMILY_NAME, tbl_Person_Aliases.FIRST_GIVEN_NAME, tbl_Person_Aliases.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN tbl_Person_Aliases ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = tbl_Person_Aliases.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ia_1_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ta_1_1.PERSON_ID,
                                                                                    ta_1_1.FAMILY_NAME,
                                                                                    ta_1_1.FIRST_GIVEN_NAME,
                                                                                    ta_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((tbl_Person_Aliases AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN tbl_Person_Aliases AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ta_1_1
                                                                            ) sq_1 INNER JOIN (
                                                                                SELECT DISTINCT
                                                                                     ari_2_1.PERSON_ID,
                                                                                     ari_2_1.FAMILY_NAME,
                                                                                     ari_2_1.FIRST_GIVEN_NAME,
                                                                                     ari_2_1.SECOND_GIVEN_NAME
                                                                                 FROM (
                                                                                        SELECT DISTINCT RAPIDAPP_PERSON_ROSTER.PERSON_ID, RAPIDAPP_PERSON_ROSTER.FAMILY_NAME, RAPIDAPP_PERSON_ROSTER.FIRST_GIVEN_NAME, RAPIDAPP_PERSON_ROSTER.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN RAPIDAPP_PERSON_ROSTER ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = RAPIDAPP_PERSON_ROSTER.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                     ) ari_2_1
                                                                                 UNION
                                                                                 SELECT DISTINCT
                                                                                     art_2_1.PERSON_ID,
                                                                                     art_2_1.FAMILY_NAME,
                                                                                     art_2_1.FIRST_GIVEN_NAME,
                                                                                     art_2_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((RAPIDAPP_PERSON_ROSTER AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN RAPIDAPP_PERSON_ROSTER AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) art_2_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ia_2_1.PERSON_ID,
                                                                                    ia_2_1.FAMILY_NAME,
                                                                                    ia_2_1.FIRST_GIVEN_NAME,
                                                                                    ia_2_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT tbl_Person_Aliases.PERSON_ID, tbl_Person_Aliases.FAMILY_NAME, tbl_Person_Aliases.FIRST_GIVEN_NAME, tbl_Person_Aliases.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN tbl_Person_Aliases ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = tbl_Person_Aliases.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ia_2_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ta_2_1.PERSON_ID,
                                                                                    ta_2_1.FAMILY_NAME,
                                                                                    ta_2_1.FIRST_GIVEN_NAME,
                                                                                    ta_2_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((tbl_Person_Aliases AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN tbl_Person_Aliases AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ta_2_1
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
                                                                             IIf(sq_1_1.FAMILY_NAME Is Not Null,Trim(sq_1_1.FAMILY_NAME),'') AS Search_Person_Last_Name,
                                                                             IIf(sq_1_1.FIRST_GIVEN_NAME Is Not Null,Trim(sq_1_1.FIRST_GIVEN_NAME),'') AS Search_Person_First_Name,
                                                                             IIf(sq_1_1.SECOND_GIVEN_NAME Is Not Null,Left(sq_1_1.SECOND_GIVEN_NAME,1),'') AS Search_Person_Middle_Name,
                                                                             Trim(aol_1_1.ACTSI_ORGANIZATION_NAME) AS ACTSI_Organization""",
                                                         'fromClause' : """
                                                                           FROM
                                                                               (
                                                                                   SELECT DISTINCT
                                                                                     ari_1_1_1.PERSON_ID,
                                                                                     ari_1_1_1.FAMILY_NAME,
                                                                                     ari_1_1_1.FIRST_GIVEN_NAME,
                                                                                     ari_1_1_1.SECOND_GIVEN_NAME
                                                                                 FROM (
                                                                                        SELECT DISTINCT RAPIDAPP_PERSON_ROSTER.PERSON_ID, RAPIDAPP_PERSON_ROSTER.FAMILY_NAME, RAPIDAPP_PERSON_ROSTER.FIRST_GIVEN_NAME, RAPIDAPP_PERSON_ROSTER.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN RAPIDAPP_PERSON_ROSTER ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = RAPIDAPP_PERSON_ROSTER.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                     ) ari_1_1_1
                                                                                 UNION
                                                                                 SELECT DISTINCT
                                                                                     art_1_1_1.PERSON_ID,
                                                                                     art_1_1_1.FAMILY_NAME,
                                                                                     art_1_1_1.FIRST_GIVEN_NAME,
                                                                                     art_1_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((RAPIDAPP_PERSON_ROSTER AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN RAPIDAPP_PERSON_ROSTER AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) art_1_1_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ia_1_1_1.PERSON_ID,
                                                                                    ia_1_1_1.FAMILY_NAME,
                                                                                    ia_1_1_1.FIRST_GIVEN_NAME,
                                                                                    ia_1_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT tbl_Person_Aliases.PERSON_ID, tbl_Person_Aliases.FAMILY_NAME, tbl_Person_Aliases.FIRST_GIVEN_NAME, tbl_Person_Aliases.SECOND_GIVEN_NAME
                                                                                        FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN tbl_Person_Aliases ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = tbl_Person_Aliases.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                        WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ia_1_1_1
                                                                                UNION
                                                                                SELECT DISTINCT
                                                                                    ta_1_1_1.PERSON_ID,
                                                                                    ta_1_1_1.FAMILY_NAME,
                                                                                    ta_1_1_1.FIRST_GIVEN_NAME,
                                                                                    ta_1_1_1.SECOND_GIVEN_NAME
                                                                                FROM (
                                                                                        SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                        FROM (((tbl_Person_Aliases AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN tbl_Person_Aliases AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                        WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                    ) ta_1_1_1
                                                                               )  AS sq_1_1, RAPIDAPP_ACTSI_ORGANIZATION_LOOKUP aol_1_1
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
                                                                      Iif(sq_1.FAMILY_NAME Is Not Null, Trim(sq_1.FAMILY_NAME), '') AS Search_Person_Last_Name,
                                                                      Iif(sq_1.FIRST_GIVEN_NAME Is Not Null, Trim(sq_1.FIRST_GIVEN_NAME), '') AS Search_Person_First_Name,
                                                                      Iif(sq_1.SECOND_GIVEN_NAME Is Not Null, Left(sq_1.SECOND_GIVEN_NAME, 1), '') AS Search_Person_Middle_Name,
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
                                                                            FROM (((((
                                                                                RAPIDAPP_RESEARCH_GRANT rg_1_1 INNER JOIN RAPIDAPP_PROJECT_GRANT pg_1_1 ON
                                                                                    rg_1_1.RESEARCH_GRANT_ID = pg_1_1.RESEARCH_GRANT_ID)
                                                                                INNER JOIN RAPIDAPP_RESEARCH_PROJECT rp_1_1 ON
                                                                                    pg_1_1.RESEARCH_PROJECT_ID = rp_1_1.RESEARCH_PROJECT_ID)
                                                                                INNER JOIN RAPIDAPP_PERSON_PROJECT_ROLE ppr_1_1 ON
                                                                                    rp_1_1.RESEARCH_PROJECT_ID = ppr_1_1.RESEARCH_PROJECT_ID)
                                                                                INNER JOIN (
                                                                                    SELECT DISTINCT
                                                                                         ari_1_1_1.PERSON_ID,
                                                                                         ari_1_1_1.FAMILY_NAME,
                                                                                         ari_1_1_1.FIRST_GIVEN_NAME,
                                                                                         ari_1_1_1.SECOND_GIVEN_NAME
                                                                                     FROM (
                                                                                            SELECT DISTINCT RAPIDAPP_PERSON_ROSTER.PERSON_ID, RAPIDAPP_PERSON_ROSTER.FAMILY_NAME, RAPIDAPP_PERSON_ROSTER.FIRST_GIVEN_NAME, RAPIDAPP_PERSON_ROSTER.SECOND_GIVEN_NAME
                                                                                            FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN RAPIDAPP_PERSON_ROSTER ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = RAPIDAPP_PERSON_ROSTER.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                            WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                         ) ari_1_1_1
                                                                                     UNION
                                                                                     SELECT DISTINCT
                                                                                         art_1_1_1.PERSON_ID,
                                                                                         art_1_1_1.FAMILY_NAME,
                                                                                         art_1_1_1.FIRST_GIVEN_NAME,
                                                                                         art_1_1_1.SECOND_GIVEN_NAME
                                                                                    FROM (
                                                                                            SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                            FROM (((RAPIDAPP_PERSON_ROSTER AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                    ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN RAPIDAPP_PERSON_ROSTER AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                            WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                        ) art_1_1_1
                                                                                    UNION
                                                                                    SELECT DISTINCT
                                                                                        ia_1_1_1.PERSON_ID,
                                                                                        ia_1_1_1.FAMILY_NAME,
                                                                                        ia_1_1_1.FIRST_GIVEN_NAME,
                                                                                        ia_1_1_1.SECOND_GIVEN_NAME
                                                                                    FROM (
                                                                                            SELECT DISTINCT tbl_Person_Aliases.PERSON_ID, tbl_Person_Aliases.FAMILY_NAME, tbl_Person_Aliases.FIRST_GIVEN_NAME, tbl_Person_Aliases.SECOND_GIVEN_NAME
                                                                                            FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN tbl_Person_Aliases ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = tbl_Person_Aliases.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                            WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                        ) ia_1_1_1
                                                                                    UNION
                                                                                    SELECT DISTINCT
                                                                                        ta_1_1_1.PERSON_ID,
                                                                                        ta_1_1_1.FAMILY_NAME,
                                                                                        ta_1_1_1.FIRST_GIVEN_NAME,
                                                                                        ta_1_1_1.SECOND_GIVEN_NAME
                                                                                    FROM (
                                                                                            SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                            FROM (((tbl_Person_Aliases AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                    ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                    (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN tbl_Person_Aliases AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                            WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                        ) ta_1_1_1
                                                                                ) AS sq_1_1 ON
                                                                                    ppr_1_1.PERSON_ID = sq_1_1.PERSON_ID)
                                                                                INNER JOIN RAPIDAPP_FUNDING_ORGANIZATION_LOOKUP fol_1_1 ON
                                                                                    rg_1_1.FUNDING_ORGANIZATION_ID = fol_1_1.FUNDING_ORGANIZATION_ID)
                                                                            WHERE fol_1_1.FUNDING_ORGANIZATION_TYPE_CODE = 1975
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
                                                                                   Iif(pr_1_1.FAMILY_NAME Is Not Null, Trim(pr_1_1.FAMILY_NAME), '') AS Search_Person_Last_Name,
                                                                                   Iif(pr_1_1.FIRST_GIVEN_NAME Is Not Null, Trim(pr_1_1.FIRST_GIVEN_NAME), '') AS Search_Person_First_Name,
                                                                                   Iif(pr_1_1.SECOND_GIVEN_NAME Is Not Null, Left(pr_1_1.SECOND_GIVEN_NAME, 1), '') AS Search_Person_Middle_Name,
                                                                                   ca_1_1.Coauthor_ID,
                                                                                   Iif(ca_1_1.LastName Is Not Null, Trim(ca_1_1.LastName), '') AS Coauthor_Last_Name,
                                                                                   Iif(ca_1_1.FirstName Is Not Null, Trim(ca_1_1.FirstName), '') AS Coauthor_First_Name,
                                                                                   Iif(ca_1_1.Initials Is Not Null, Trim(ca_1_1.Initials), '') AS Coauthor_Initials
                                                                               FROM ((((
                                                                                       SELECT DISTINCT
                                                                                             ari_1_1_1.PERSON_ID,
                                                                                             ari_1_1_1.FAMILY_NAME,
                                                                                             ari_1_1_1.FIRST_GIVEN_NAME,
                                                                                             ari_1_1_1.SECOND_GIVEN_NAME
                                                                                         FROM (
                                                                                                SELECT DISTINCT RAPIDAPP_PERSON_ROSTER.PERSON_ID, RAPIDAPP_PERSON_ROSTER.FAMILY_NAME, RAPIDAPP_PERSON_ROSTER.FIRST_GIVEN_NAME, RAPIDAPP_PERSON_ROSTER.SECOND_GIVEN_NAME
                                                                                                FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN RAPIDAPP_PERSON_ROSTER ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = RAPIDAPP_PERSON_ROSTER.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                                WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                             ) ari_1_1_1
                                                                                         UNION
                                                                                         SELECT DISTINCT
                                                                                             art_1_1_1.PERSON_ID,
                                                                                             art_1_1_1.FAMILY_NAME,
                                                                                             art_1_1_1.FIRST_GIVEN_NAME,
                                                                                             art_1_1_1.SECOND_GIVEN_NAME
                                                                                        FROM (
                                                                                                SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                                FROM (((RAPIDAPP_PERSON_ROSTER AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                        ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN RAPIDAPP_PERSON_ROSTER AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                                WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                            ) art_1_1_1
                                                                                        UNION
                                                                                        SELECT DISTINCT
                                                                                            ia_1_1_1.PERSON_ID,
                                                                                            ia_1_1_1.FAMILY_NAME,
                                                                                            ia_1_1_1.FIRST_GIVEN_NAME,
                                                                                            ia_1_1_1.SECOND_GIVEN_NAME
                                                                                        FROM (
                                                                                                SELECT DISTINCT tbl_Person_Aliases.PERSON_ID, tbl_Person_Aliases.FAMILY_NAME, tbl_Person_Aliases.FIRST_GIVEN_NAME, tbl_Person_Aliases.SECOND_GIVEN_NAME
                                                                                                FROM (RAPIDAPP_INVESTIGATOR_PROGRAM INNER JOIN tbl_Person_Aliases ON RAPIDAPP_INVESTIGATOR_PROGRAM.PERSON_ID = tbl_Person_Aliases.PERSON_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP ON (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE And (RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)>=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_BEGIN_DATE)<=RAPIDAPP_PROJECT_YEAR_LOOKUP.REPORTING_YEAR_START_DATE) AND ((RAPIDAPP_INVESTIGATOR_PROGRAM.SUPPORT_COMPLETED_DATE) Is Null))
                                                                                                WHERE RAPIDAPP_PROJECT_YEAR_LOOKUP.PROJECT_YEAR_NUMBER>=1
                                                                                            ) ia_1_1_1
                                                                                        UNION
                                                                                        SELECT DISTINCT
                                                                                            ta_1_1_1.PERSON_ID,
                                                                                            ta_1_1_1.FAMILY_NAME,
                                                                                            ta_1_1_1.FIRST_GIVEN_NAME,
                                                                                            ta_1_1_1.SECOND_GIVEN_NAME
                                                                                        FROM (
                                                                                                SELECT DISTINCT pr_1.PERSON_ID, pr_1.FAMILY_NAME, pr_1.FIRST_GIVEN_NAME, pr_1.SECOND_GIVEN_NAME
                                                                                                FROM (((tbl_Person_Aliases AS pr_1 INNER JOIN RAPIDAPP_TRAINING_ROSTER AS tr_1 ON pr_1.PERSON_ID = tr_1.PERSON_ID) INNER JOIN RAPIDAPP_PERSONNEL_TYPE_CODE AS ptc_1 ON tr_1.PERSONNEL_TYPE_CODE = ptc_1.CONCEPT_ID) INNER JOIN RAPIDAPP_PROJECT_YEAR_LOOKUP AS pyl_1 ON (
                                                                                                        ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE And (tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_END_DATE)>=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE)<=pyl_1.REPORTING_YEAR_END_DATE)) OR
                                                                                                        (((tr_1.APPOINTMENT_START_DATE)<=pyl_1.REPORTING_YEAR_START_DATE) AND ((tr_1.APPOINTMENT_END_DATE) Is Null))) LEFT JOIN tbl_Person_Aliases AS mpr_1 ON tr_1.MENTOR_PERSON_ID = mpr_1.PERSON_ID
                                                                                                WHERE pyl_1.PROJECT_YEAR_NUMBER>=1
                                                                                            ) ta_1_1_1
                                                                                   ) pr_1_1 INNER JOIN tbl_Investigator_Publications ip_1_1 ON
                                                                                       pr_1_1.PERSON_ID = ip_1_1.Investigator_ID)
                                                                                   INNER JOIN tbl_Publication_Coauthors pca_1_1 ON
                                                                                       ip_1_1.Publication_ID = pca_1_1.Publication_ID)
                                                                                   INNER JOIN tbl_Coauthors ca_1_1 ON
                                                                                       pca_1_1.Coauthor_ID = ca_1_1.Coauthor_ID)
                                                                                WHERE (Trim(ca_1_1.LastName) NOT LIKE '*' + Trim(pr_1_1.FAMILY_NAME) + '*' AND
                                                                                Left(Trim(ca_1_1.FirstName), 1) <> Left(Trim(pr_1_1.FIRST_GIVEN_NAME), 1)) OR
                                                                                (Trim(pr_1_1.FAMILY_NAME) NOT LIKE '*' + Trim(ca_1_1.LastName) + '*' AND
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
                                                                              sq_1.Coauthor_Initials"""}}
    
    search_terms_resultset = None
    
    current_person_id = None
    current_coinvestigator_id = None
    current_coauthor_id = None

    current_running_flag = None
    
    def get_search_recordset(self, search_strategy_in):
        
        return self.run_sql_query(
                                  "SELECT " +
                                  self.search_method_query_parts[search_strategy_in]['selectFields'] + " " +
                                  self.search_method_query_parts[search_strategy_in]['fromClause'] + " " +
                                  self.search_method_query_parts[search_strategy_in]['orderByClause'] + ";")
    
    def get_search_recordcount(self, search_strategy_in):
        
        return_recordcount = 0
        
        count_recordset = self.run_sql_query(
                                             "SELECT COUNT(*) AS Search_Term_Count " +
                                             self.search_method_query_parts[search_strategy_in]['fromClause'] + ";")
        
        count_recordset.next()
        return_recordcount = count_recordset.getInt('Search_Term_Count')
        count_recordset.close()
        
        return return_recordcount
        
    def get_search_term(self):
        
        return_search_term = None
        
        if self.search_terms_resultset == None:
            self.search_terms_resultset = self.get_search_recordset(self.current_running_flag)
        
        if self.search_terms_resultset.next():
                        
            running_flag_case = self.current_running_flag                    
            if running_flag_case == self.PERSON_BY_PERSON:
                    
                search_person_term = None
                co_investigator_term = None
                    
                self.current_person_id = self.search_terms_resultset.getInt('Search_Person_ID')
                self.current_coinvestigator_id = self.search_terms_resultset.getInt('Co_Investigator_ID')
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      self.search_terms_resultset.getString('Search_Person_Last_Name').strip() + ', ' +
                                      self.search_terms_resultset.getString('Search_Person_First_Name').strip() + ' ' +
                                      self.search_terms_resultset.getString('Search_Person_Middle_Name').strip())
                    
                co_investigator_term = (
                                        self.search_terms_resultset.getString('Co_Investigator_Last_Name').strip() + ', ' +
                                        self.search_terms_resultset.getString('Co_Investigator_First_Name').strip() + ' ' +
                                        self.search_terms_resultset.getString('Co_Investigator_Middle_Name').strip())
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + co_investigator_term.strip() + '[au])'
                    
            elif running_flag_case == self.PERSON_ORGANIZATION:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = self.search_terms_resultset.getInt('Search_Person_ID')
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      self.search_terms_resultset.getString('Search_Person_Last_Name').strip() + ', ' +
                                      self.search_terms_resultset.getString('Search_Person_First_Name').strip() + ' ' +
                                      self.search_terms_resultset.getString('Search_Person_Middle_Name').strip())
                    
                organization_term = self.search_terms_resultset.getString('ACTSI_Organization').strip()
                    
                return_search_term = '(' + search_person_term.strip() + '[au]) and (' + organization_term.strip() + '[ad])'
                    
            elif running_flag_case == self.PERSON_GRANT:
                    
                search_person_term = None
                organization_term = None
                    
                self.current_person_id = self.search_terms_resultset.getInt('Search_Person_ID')
                self.current_coinvestigator_id = None
                self.current_coauthor_id = None
                    
                search_person_term = (
                                      self.search_terms_resultset.getString('Search_Person_Last_Name').strip() + ', ' +
                                      self.search_terms_resultset.getString('Search_Person_First_Name').strip() + ' ' +
                                      self.search_terms_resultset.getString('Search_Person_Middle_Name').strip())
                    
                return_search_term = ('(' + search_person_term.strip() + '[au]) and (' + self.search_terms_resultset.getString('Grant_Number').strip() + '[gr])')
                    
            elif running_flag_case == self.PERSON_COAUTHOR:
                    
                search_person_term = None
                co_author_term = None
                    
                co_author_last_name = None
                    
                self.current_person_id = self.search_terms_resultset.getInt('Search_Person_ID')
                self.current_coinvestigator_id = None
                self.current_coauthor_id = self.search_terms_resultset.getInt('Coauthor_ID')
                    
                search_person_term = (
                                      self.search_terms_resultset.getString('Search_Person_Last_Name').strip() + ', ' +
                                      self.search_terms_resultset.getString('Search_Person_First_Name').strip() + ' ' +
                                      self.search_terms_resultset.getString('Search_Person_Middle_Name').strip())
                    
                co_author_last_name = self.search_terms_resultset.getString('Coauthor_Last_Name').strip()
                    
                co_author_term = (
                                  '(' +
                                  co_author_last_name.strip() + ', ' +
                                  self.search_terms_resultset.getString('Coauthor_First_Name').strip() + '[au]) and (' +
                                  co_author_last_name.strip() + ' ' +
                                  self.search_terms_resultset.getString('Coauthor_Initials').strip() + '[au])')
                    
                return_search_term = '(' + co_author_term.strip() + ') and (' + search_person_term.strip() + '[au])'
                    
        else:
            self.search_terms_resultset = None
            
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
    
class PubMedAccessJDBCODBCDatabaseManager(PubMedAccessJDBCODBCDatabaseController):
    
    database_table_parts = [
                            {
                             'tableName' : 'tbl_PubMed_Publications',
                             'tableColumns' : """
                                                 Publication_ID COUNTER PRIMARY KEY,
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
                                                 Author_List_Complete YESNO,
                                                 Grant_List_Complete YESNO,
                                                 PubMed_XML MEMO,
                                                 PubMed_XML_Date DATETIME"""},
                            {
                             'tableName' : 'tbl_Investigator_Publications',
                             'tableColumns' : """
                                                 Investigator_Publications_ID COUNTER PRIMARY KEY,
                                                 Investigator_ID INTEGER,
                                                 Publication_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Grants',
                             'tableColumns' : """
                                                 Grant_ID COUNTER PRIMARY KEY,
                                                 Grant_Number TEXT,
                                                 PHS_Activity_Code TEXT,
                                                 PHS_Organization TEXT,
                                                 PHS_Six_Digit_Grant_Number TEXT,
                                                 Acronym TEXT,
                                                 Agency TEXT,
                                                 Country TEXT,
                                                 CTSA_Grant YESNO"""},
                            {
                             'tableName' : 'tbl_Publication_Grants_Cited',
                             'tableColumns' : """
                                                 Publication_Grants_Cited_ID COUNTER PRIMARY KEY,
                                                 Publication_ID INTEGER,
                                                 Grant_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Coauthors',
                             'tableColumns' : """
                                                 Coauthor_ID COUNTER PRIMARY KEY,
                                                 LastName TEXT,
                                                 FirstName TEXT,
                                                 Initials TEXT,
                                                 Author_Valid YESNO"""},
                            {
                             'tableName' : 'tbl_Publication_Coauthors',
                             'tableColumns' : """
                                                 Publication_Coauthor_ID COUNTER PRIMARY KEY,
                                                 Publication_ID INTEGER,
                                                 Coauthor_ID INTEGER"""},
                            {
                             'tableName' : 'tbl_Mesh_Descriptors',
                             'tableColumns' : """
                                                 Mesh_Descriptor_ID COUNTER PRIMARY KEY,
                                                 DescriptorUI TEXT,
                                                 Mesh_Descriptor TEXT"""},
                            {
                             'tableName' : 'tbl_Mesh_Qualifiers',
                             'tableColumns' : """
                                                 Mesh_Qualifier_ID COUNTER PRIMARY KEY,
                                                 QualifierUI TEXT,
                                                 Mesh_Qualifier TEXT"""},
                            {
                             'tableName' : 'tbl_Publication_Mesh_Terms',
                             'tableColumns' : """
                                                 Publication_Mesh_Term_ID COUNTER PRIMARY KEY,
                                                 Publication_ID INTEGER,
                                                 Mesh_Descriptor_ID INTEGER,
                                                 Major_Descriptor YESNO,
                                                 Mesh_Qualifier_ID INTEGER,
                                                 Major_Qualifier YESNO"""},
                            {
                             'tableName' : 'tbl_Investigator_Coauthors_Searched',
                             'tableColumns' : """
                                                 Investigator_Coauthor_ID COUNTER PRIMARY KEY,
                                                 Investigator_ID INTEGER,
                                                 Coauthor_ID INTEGER"""}]
    
    def delete_publications_tables(self):
        
        db_statement = None
        
        try:
            db_statement = self.database_connection.createStatement()
        except AttributeError:
            return False
        
        for database_table_item in self.database_table_parts:
            
            try:
                db_statement.execute("DROP TABLE " + database_table_item['tableName'] + ";")
                self.database_connection.commit()
                print('Dropped table: ' + database_table_item['tableName'].strip())
            except SQLException, sqlex:
                if sqlex.getErrorCode() == -1305:
                    print('Drop table skipped: ' + database_table_item['tableName'].strip())
                else:
                    raise sqlex
                
        db_statement.close()
        
        return True
        
    def create_publications_tables(self):
        
        db_statement = None
        
        try:
            db_statement = self.database_connection.createStatement()
        except AttributeError:
            return False
            
        try:
            db_statement.execute("DROP TABLE tbl_Investigator_Coauthors_Searched;")
            self.database_connection.commit()
            print('Dropped table: tbl_Investigator_Coauthors_Searched')
        except SQLException, sqlex:
            if sqlex.getErrorCode() == -1305:
                print('Drop table skipped: tbl_Investigator_Coauthors_Searched')
            else:
                raise sqlex
        
        for database_table_item in self.database_table_parts:
            
            try:
                db_statement.execute("CREATE TABLE " + database_table_item['tableName'] + "(" + database_table_item['tableColumns'] + ");")
                self.database_connection.commit()
                print('Created table: ' + database_table_item['tableName'].strip())
            except SQLException, sqlex:
                
                if sqlex.getErrorCode() == -1303:
                    print('Create table skipped: ' + database_table_item['tableName'].strip())
                else:
                    raise sqlex
        
        db_statement.close()
        
        return True
   
    def lookup_publication_id_by_pubmed_id(self, pubmed_id_in):
        
        db_pstatement = None
        db_result = None
        return_id = None
        
        db_pstatement = self.database_connection.prepareStatement("SELECT Publication_ID FROM tbl_PubMed_Publications WHERE PubMed_ID = ?;")
        
        query_pubmed_id = None
        try:
            query_pubmed_id = pubmed_id_in.strip()
        except TypeError:
            pass
            
        db_pstatement.setString(1, query_pubmed_id)
        
        db_result = db_pstatement.executeQuery()
        if db_result.next():
            return_id = db_result.getInt('Publication_ID')
        
        db_result.close()
        db_pstatement.close()
        
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
        
        db_pstatement = None
        
        return_publication_id = self.lookup_publication_id_by_pubmed_id(pubmed_id_in)
                
        if return_publication_id:
            
            sql_query = None
            sql_query_index = 6
            
            sql_query_fields = """
                                  Journal = ?,
                                  Medline_Date = ?,
                                  Authors = ?,
                                  Title = ?,
                                  Affiliation = ?,
                                  PubMed_XML = ?,
                                  PubMed_XML_Date = Now()"""
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID = ?"
            
            if volume_in != None:
                sql_query_fields += ", Volume = ?"
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number = ?"
                
            if pagination_in != None:
                sql_query_fields += ", Pages = ?"
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year = ?"
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month = ?"
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete = ?"
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete = ?"
                    
            sql_query = "UPDATE tbl_PubMed_Publications SET " + sql_query_fields + " WHERE Publication_ID = ?;"
            
            db_pstatement = self.database_connection.prepareStatement(sql_query)
            
            db_pstatement.setString(1, journal_title_in)
            db_pstatement.setString(2, medline_date_in)
            db_pstatement.setString(3, authors_in)
            db_pstatement.setString(4, article_title_in)
            db_pstatement.setString(5, affiliation_in)
            db_pstatement.setString(6, pubmed_xml_in)
            
            if pmcid_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, pmcid_in)
            
            if volume_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, volume_in)
                
            if issue_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, issue_in)
                
            if pagination_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, pagination_in)
            
            if article_year_in != None:
                sql_query_index += 1
                db_pstatement.setInt(sql_query_index, article_year_in)
                
            if article_month_in != None:
                sql_query_index += 1
                db_pstatement.setInt(sql_query_index, article_month_in)
                
            if author_list_complete_in != None:
                sql_query_index += 1
                db_pstatement.setByte(sql_query_index, author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_index += 1
                db_pstatement.setByte(sql_query_index, grant_list_complete_in)
                    
            sql_query_index += 1
            db_pstatement.setInt(sql_query_index, return_publication_id)
            
            db_pstatement.executeUpdate()
            
            print('Updated Publication ID ' + str(return_publication_id))
            
        else:
            
            db_result = None
            sql_query = None
            
            sql_query_index = 7
            
            sql_query_fields = """
                                  PubMed_ID,
                                  Journal,
                                  Medline_Date,
                                  Authors,
                                  Title,
                                  Affiliation,
                                  PubMed_XML,
                                  PubMed_XML_Date"""
            
            sql_query_values = "?, ?, ?, ?, ?, ?, ?, Now()"
            
            if pmcid_in != None:
                sql_query_fields += ", PMCID"
                sql_query_values += ", ?"
            
            if volume_in != None:
                sql_query_fields += ", Volume"
                sql_query_values += ", ?"
                
            if issue_in != None:
                sql_query_fields += ", Issue_Number"
                sql_query_values += ", ?"
                
            if pagination_in != None:
                sql_query_fields += ", Pages"
                sql_query_values += ", ?"
            
            if article_year_in != None:
                sql_query_fields += ", Publish_Year"
                sql_query_values += ", ?"
                
            if article_month_in != None:
                sql_query_fields += ", Publish_Month"
                sql_query_values += ", ?"
                
            if author_list_complete_in != None:
                sql_query_fields += ", Author_List_Complete"
                sql_query_values += ", ?"
                
            if grant_list_complete_in != None:
                sql_query_fields += ", Grant_List_Complete"
                sql_query_values += ", ?"
                
            sql_query = (
                         "INSERT INTO tbl_PubMed_Publications (" +
                         sql_query_fields +
                         ") VALUES (" +
                         sql_query_values +
                         ");")
                                                                         
            db_pstatement = self.database_connection.prepareStatement(sql_query)
            
            db_pstatement.setString(1, pubmed_id_in)
            db_pstatement.setString(2, journal_title_in)
            db_pstatement.setString(3, medline_date_in)
            db_pstatement.setString(4, authors_in)
            db_pstatement.setString(5, article_title_in)
            db_pstatement.setString(6, affiliation_in)
            db_pstatement.setString(7, pubmed_xml_in)
            
            if pmcid_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, pmcid_in)
            
            if volume_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, volume_in)
                
            if issue_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, issue_in)
                
            if pagination_in != None:
                sql_query_index += 1
                db_pstatement.setString(sql_query_index, pagination_in)
            
            if article_year_in != None:
                sql_query_index += 1
                db_pstatement.setInt(sql_query_index, article_year_in)
                
            if article_month_in != None:
                sql_query_index += 1
                db_pstatement.setInt(sql_query_index, article_month_in)
                
            if author_list_complete_in != None:
                sql_query_index += 1
                db_pstatement.setByte(sql_query_index, author_list_complete_in)
                
            if grant_list_complete_in != None:
                sql_query_index += 1
                db_pstatement.setByte(sql_query_index, grant_list_complete_in)
            
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
            db_pstatement = self.database_connection.prepareStatement("SELECT @@IDENTITY AS Publication_ID;")
            
            db_result = db_pstatement.executeQuery()
            
            if db_result.next():
                return_publication_id = db_result.getInt('Publication_ID')
                
            print('Inserted Publication ID ' + str(return_publication_id))
        
            db_result.close()
            
        db_pstatement.close()
        
        return return_publication_id
    
    def get_coauthor_id(self, last_name_in, first_name_in, initials_in):
        
        return_coauthor_id = None
        
        db_pstatement = None
        db_result = None
        
        parameter_index = 0
        
        sql_query = """
                       SELECT Coauthor_ID
                       FROM tbl_Coauthors
                       WHERE """
        
        if last_name_in:
            sql_query += " UCase(Trim(?)) = UCase(Trim(LastName)) "
        else:
            sql_query += " LastName IS NULL "
        
        if first_name_in:
            sql_query += " AND UCase(Trim(?)) = UCase(Trim(FirstName)) "
        else:
            sql_query += " AND FirstName IS NULL "
        
        if initials_in:
            sql_query += " AND UCase(Trim(?)) = UCase(Trim(Initials)) "
        else:
            sql_query += " AND Initials IS NULL "
            
        sql_query += ";"
        
        db_pstatement = self.database_connection.prepareStatement(sql_query)
        
        if last_name_in:
            parameter_index += 1
            db_pstatement.setString(parameter_index, last_name_in)
            
        if first_name_in:
            parameter_index += 1
            db_pstatement.setString(parameter_index, first_name_in)
            
        if initials_in:
            parameter_index += 1
            db_pstatement.setString(parameter_index, initials_in)
            
        db_result = db_pstatement.executeQuery()
            
        if db_result.next():
            return_coauthor_id = db_result.getInt('Coauthor_ID')
            db_result.close()
            
        db_pstatement.close()
        
        if not return_coauthor_id:
            
            db_pstatement = self.database_connection.prepareStatement("INSERT INTO tbl_Coauthors (LastName, FirstName, Initials) VALUES (?, ?, ?);")
            
            db_pstatement.setString(1, last_name_in)
            db_pstatement.setString(2, first_name_in)
            db_pstatement.setString(3, initials_in)
            
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
            db_pstatement = self.database_connection.prepareStatement("SELECT @@IDENTITY AS Coauthor_ID;")
            
            db_result = db_pstatement.executeQuery()
            
            if db_result.next():
                return_coauthor_id = db_result.getInt('Coauthor_ID')
                db_result.close()
                
            db_pstatement.close()
        
        return return_coauthor_id
    
    def join_publication_coauthor(self, publication_id_in, coauthor_id_in):
        
        db_pstatement = None
        db_result = None
        
        check_publication_coauthor = 0
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT COUNT(*) AS Publication_Coauthor_Count
                                                                     FROM tbl_Publication_Coauthors
                                                                     WHERE
                                                                         Publication_ID = ? AND
                                                                         Coauthor_ID = ?
                                                                     ;""")
        
        db_pstatement.setInt(1, publication_id_in)
        db_pstatement.setInt(2, coauthor_id_in)
        
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            check_publication_coauthor = db_result.getInt('Publication_Coauthor_Count')
            db_result.close()
        
        db_pstatement.close()
        
        if check_publication_coauthor == 0:
            
            db_pstatement = self.database_connection.prepareStatement("""
                                                                         INSERT INTO tbl_Publication_Coauthors (
                                                                             Publication_ID, Coauthor_ID
                                                                         ) VALUES (
                                                                             ?, ?
                                                                         );""")
            
            db_pstatement.setInt(1, publication_id_in)
            db_pstatement.setInt(2, coauthor_id_in)
            
            db_pstatement.executeUpdate()
            
            db_pstatement.close()
        
    def get_mesh_descriptor_id (self, mesh_descriptor_in):
        
        db_pstatement = None
        db_result = None
        
        mesh_descriptor_id = None
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT Mesh_Descriptor_ID
                                                                     FROM tbl_Mesh_Descriptors
                                                                     WHERE UCase(Trim(Mesh_Descriptor)) = UCase(Trim(?))
                                                                     ;""")
        
        db_pstatement.setString(1, mesh_descriptor_in)
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            mesh_descriptor_id = db_result.getInt('Mesh_Descriptor_ID')
            db_result.close()
        
        db_pstatement.close()
        
        if mesh_descriptor_id == None:
            
            db_pstatement = self.database_connection.prepareStatement("INSERT INTO tbl_Mesh_Descriptors (Mesh_Descriptor) VALUES (?);")
            
            db_pstatement.setString(1, mesh_descriptor_in)
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
            db_pstatement = self.database_connection.prepareStatement("SELECT @@IDENTITY AS Mesh_Descriptor_ID;")
            db_result = db_pstatement.executeQuery()
            
            if db_result.next():
                mesh_descriptor_id = db_result.getInt('Mesh_Descriptor_ID')
                db_result.close()
            
            db_pstatement.close()
            
        return mesh_descriptor_id
        
    def get_mesh_qualifier_id (self, mesh_qualifier_in):
        
        db_pstatement = None
        db_result = None
        
        mesh_qualifier_id = None
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT Mesh_Qualifier_ID
                                                                     FROM tbl_Mesh_Qualifiers
                                                                     WHERE UCase(Trim(Mesh_Qualifier)) = UCase(Trim(?))
                                                                     ;""")
        
        db_pstatement.setString(1, mesh_qualifier_in)
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            mesh_qualifier_id = db_result.getInt('Mesh_Qualifier_ID')
            db_result.close()
        
        db_pstatement.close()
        
        if mesh_qualifier_id == None:
            
            db_pstatement = self.database_connection.prepareStatement("INSERT INTO tbl_Mesh_Qualifiers (Mesh_Qualifier) VALUES (?);")
            
            db_pstatement.setString(1, mesh_qualifier_in)
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
            db_pstatement = self.database_connection.prepareStatement("SELECT @@IDENTITY AS Mesh_Qualifier_ID;")
            db_result = db_pstatement.executeQuery()
            
            if db_result.next():
                mesh_qualifier_id = db_result.getInt('Mesh_Qualifier_ID')
                db_result.close()
            
            db_pstatement.close()
            
        return mesh_qualifier_id
                    
    def join_publication_mesh_terms(
                                    self,
                                    publication_id_in,
                                    descriptor_id_in,
                                    descriptor_major_topic_in,
                                    qualifier_id_in,
                                    qualifier_major_topic_in):
        
        db_pstatement = None
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
            
        db_pstatement = self.database_connection.prepareStatement(sql_query)
        
        db_pstatement.setInt(1, publication_id_in)
        db_pstatement.setInt(2, descriptor_id_in)
        
        if qualifier_id_in != None:
            db_pstatement.setInt(3, qualifier_id_in)
            
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            publication_mesh_term_id = db_result.getInt('Publication_Mesh_Term_ID')
            db_result.close()
            
        db_pstatement.close()
            
        if publication_mesh_term_id == None:
            
            sql_query_fields = "Publication_ID, Mesh_Descriptor_ID, Major_Descriptor"
            sql_query_values = "?, ?, ?"
            query_values_index = 3
            
            if qualifier_id_in != None:
                sql_query_fields += ", Mesh_Qualifier_ID"
                sql_query_values += ", ?"
                
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier"
                sql_query_values += ", ?"
                
            db_pstatement = self.database_connection.prepareStatement("INSERT INTO tbl_Publication_Mesh_Terms (" + sql_query_fields + ") VALUES (" + sql_query_values + ");")
            
            db_pstatement.setInt(1, publication_id_in)
            db_pstatement.setInt(2, descriptor_id_in)
            db_pstatement.setByte(3, descriptor_major_topic_in)
            
            if qualifier_id_in != None:
                query_values_index += 1
                db_pstatement.setInt(query_values_index, qualifier_id_in)
                
            if qualifier_major_topic_in != None:
                query_values_index += 1
                db_pstatement.setByte(query_values_index, qualifier_major_topic_in)
            
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
        else:
            
            sql_query_fields = "Major_Descriptor = ?"
            query_values_index = 1
            
            if qualifier_major_topic_in != None:
                sql_query_fields += ", Major_Qualifier = ?"
                
            db_pstatement = self.database_connection.prepareStatement("UPDATE tbl_Publication_Mesh_Terms SET " + sql_query_fields + " WHERE Publication_Mesh_Term_ID = ?;")
            
            db_pstatement.setByte(query_values_index, descriptor_major_topic_in)
            
            if qualifier_major_topic_in != None:
                query_values_index += 1
                db_pstatement.setByte(query_values_index, qualifier_major_topic_in)
            
            query_values_index += 1
            db_pstatement.setInt(query_values_index, publication_mesh_term_id)
            
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
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
        
        db_pstatement = None
        db_result = None
        
        return_grant_id = None
        
        query_values_index = 1
        
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
            
        db_pstatement = self.database_connection.prepareStatement(sql_query)
        
        db_pstatement.setString(query_values_index, grant_number_in)
        
        if phs_activity_code_in != None:
            query_values_index += 1
            db_pstatement.setString(query_values_index, phs_activity_code_in)
            
        if phs_organization_in != None:
            query_values_index += 1
            db_pstatement.setString(query_values_index, phs_organization_in)
            
        if phs_grant_number_in != None:
            query_values_index += 1
            db_pstatement.setString(query_values_index, phs_grant_number_in)
            
        if agency_in != None:
            query_values_index += 1
            db_pstatement.setString(query_values_index, agency_in)
            
        if country_in != None:
            query_values_index += 1
            db_pstatement.setString(query_values_index, country_in)
            
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            return_grant_id = db_result.getInt('Grant_ID')
            db_result.close()
            
        db_pstatement.close()
            
        if return_grant_id == None:
            
            sql_query_fields = "Acronym, Agency, Country"
            sql_query_values = "?, ?, ?"
            query_values_index = 3
            
            if grant_number_in != None:
                sql_query_fields += ", Grant_Number"
                sql_query_values += ", ?"
            
            if phs_activity_code_in != None:
                sql_query_fields += ", PHS_Activity_Code"
                sql_query_values += ", ?"
                
            if phs_organization_in != None:
                sql_query_fields += ", PHS_Organization"
                sql_query_values += ", ?"
                
            if phs_grant_number_in != None:
                sql_query_fields += ", PHS_Six_Digit_Grant_Number"
                sql_query_values += ", ?"
            
            if actsi_grant_in != None:
                sql_query_fields += ", CTSA_Grant"
                sql_query_values += ", ?"
                
            db_pstatement = self.database_connection.prepareStatement("INSERT INTO tbl_Grants (" + sql_query_fields + ") VALUES (" + sql_query_values + ");")
            
            db_pstatement.setString(1, acronym_in)
            db_pstatement.setString(2, agency_in)
            db_pstatement.setString(3, country_in)
            
            if grant_number_in != None:
                query_values_index += 1
                db_pstatement.setString(query_values_index, grant_number_in)
            
            if phs_activity_code_in != None:
                query_values_index += 1
                db_pstatement.setString(query_values_index, phs_activity_code_in)
                
            if phs_organization_in != None:
                query_values_index += 1
                db_pstatement.setString(query_values_index, phs_organization_in)
                
            if phs_grant_number_in != None:
                query_values_index += 1
                db_pstatement.setString(query_values_index, phs_grant_number_in)
            
            if actsi_grant_in != None:
                query_values_index += 1
                db_pstatement.setByte(query_values_index, actsi_grant_in)
            
            db_pstatement.executeUpdate()
            db_pstatement.close()
            
            db_pstatement = self.database_connection.prepareStatement("SELECT @@IDENTITY AS Grant_ID;")
            db_result = db_pstatement.executeQuery()
        
            if db_result.next():
                return_grant_id = db_result.getInt('Grant_ID')
                db_result.close()
                
            db_pstatement.close()
            
        else:
            
            sql_query_fields = "Acronym = ?, Agency = ?, Country = ?"
            query_values_index = 3
            
            if actsi_grant_in != None:
                sql_query_fields += ", CTSA_Grant = ?"
                
            db_pstatement = self.database_connection.prepareStatement("UPDATE tbl_Grants SET " + sql_query_fields + " WHERE Grant_ID = ?;")
            
            db_pstatement.setString(1, acronym_in)
            db_pstatement.setString(2, agency_in)
            db_pstatement.setString(3, country_in)
            
            if actsi_grant_in != None:
                query_values_index += 1
                db_pstatement.setByte(query_values_index, actsi_grant_in)
            
            query_values_index += 1
            db_pstatement.setInt(query_values_index, return_grant_id)
            
            db_pstatement.close()
            
        return return_grant_id
    
    def join_publication_grant(self, publication_id_in, grant_id_in):
        
        db_pstatement = None
        db_result = None
        
        publication_grants_cited_id = None
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT
                                                                         Publication_Grants_Cited_ID
                                                                     FROM tbl_Publication_Grants_Cited
                                                                     WHERE
                                                                         Publication_ID = ? AND
                                                                         Grant_ID = ?
                                                                     ;""")
        
        db_pstatement.setInt(1, publication_id_in)
        db_pstatement.setInt(2, grant_id_in)
        
        db_result = db_pstatement.executeQuery()
    
        if db_result.next():
            publication_grants_cited_id = db_result.getInt('Publication_Grants_Cited_ID')
            db_result.close()
            
        db_pstatement.close()
            
        if publication_grants_cited_id == None:
            
            db_pstatement = self.database_connection.prepareStatement("""
                                                                         INSERT INTO tbl_Publication_Grants_Cited (
                                                                             Publication_ID, Grant_ID
                                                                         ) VALUES (
                                                                             ?, ?
                                                                         );""")
        
            db_pstatement.setInt(1, publication_id_in)
            db_pstatement.setInt(2, grant_id_in)
            
            db_pstatement.executeUpdate()
            
            db_pstatement.close()
    
    def join_investigator_publication(self, investigator_id_in, publication_id_in):
        
        db_pstatement = None
        db_result = None
        
        check_investigator_publication = 0
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT COUNT(*) AS Investigator_Publication_Count
                                                                     FROM tbl_Investigator_Publications
                                                                     WHERE
                                                                         Investigator_ID = ? AND
                                                                         Publication_ID = ?
                                                                     ;""")
        
        db_pstatement.setInt(1, investigator_id_in)
        db_pstatement.setInt(2, publication_id_in)
        
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            check_investigator_publication = db_result.getInt('Investigator_Publication_Count')
            db_result.close()
        
        db_pstatement.close()
        
        if check_investigator_publication == 0:
            
            db_pstatement = self.database_connection.prepareStatement("""
                                                                         INSERT INTO tbl_Investigator_Publications (
                                                                             Investigator_ID, Publication_ID
                                                                         ) VALUES (
                                                                             ?, ?
                                                                         );""")
            
            db_pstatement.setInt(1, investigator_id_in)
            db_pstatement.setInt(2, publication_id_in)
            
            db_pstatement.executeUpdate()
            
            db_pstatement.close()
            
            print('Added Publication ID ' + str(publication_id_in) + ' to Investigator ID ' + str(investigator_id_in))
    
    def join_investigator_coauthor_searched(self, investigator_id_in, coauthor_id_in):
        
        db_pstatement = None
        db_result = None
        
        check_investigator_coauthor = 0
        
        db_pstatement = self.database_connection.prepareStatement("""
                                                                     SELECT COUNT(*) AS Investigator_Coauthor_Count
                                                                     FROM tbl_Investigator_Coauthors_Searched
                                                                     WHERE
                                                                         Investigator_ID = ? AND
                                                                         Coauthor_ID = ?
                                                                     ;""")
        
        db_pstatement.setInt(1, investigator_id_in)
        db_pstatement.setInt(2, coauthor_id_in)
        
        db_result = db_pstatement.executeQuery()
        
        if db_result.next():
            check_investigator_coauthor = db_result.getInt('Investigator_Coauthor_Count')
            db_result.close()
        
        db_pstatement.close()
        
        if check_investigator_coauthor == 0:
            
            db_pstatement = self.database_connection.prepareStatement("""
                                                                         INSERT INTO tbl_Investigator_Coauthors_Searched (
                                                                             Investigator_ID, Coauthor_ID
                                                                         ) VALUES (
                                                                             ?, ?
                                                                         );""")
        
            db_pstatement.setInt(1, investigator_id_in)
            db_pstatement.setInt(2, coauthor_id_in)
            
            db_pstatement.executeUpdate()
            
            db_pstatement.close()