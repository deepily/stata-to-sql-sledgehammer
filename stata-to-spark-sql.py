# coding: utf-8

# In[72]:

import pandas as pd
import re

debugging = True


# ### Load Stata Source

# In[1]:

stata_file = open( "data/QprExport-2018.06.29.stata", 'r' )
stata_list = [ line.strip() for line in stata_file.readlines() ]


# In[2]:

stata_list[ 0 ]


# ### Load Pirl 'n' Parquet Names

# In[91]:

pirl_to_parquet_df = pd.read_csv( "data/raw-pirl-to-parquet-names.csv", usecols=[ "PIRL_ElementNumber", "ParquetName" ] )
pirl_to_parquet_df[ "Pirl" ] = "PIRL" + pirl_to_parquet_df.PIRL_ElementNumber
pirl_to_parquet_df.drop( "PIRL_ElementNumber", inplace=True, axis=1 )
    
print( pirl_to_parquet_df.head() )

# get as dictionary: EZ!
pirl_dictionary = dict( zip( pirl_to_parquet_df.Pirl, pirl_to_parquet_df.ParquetName ) )


# In[38]:

pirl_dictionary[ "PIRL900" ]


# In[8]:

for i in range( 5 ):
    
    print( stata_list[ i ] )


# ### Build Stop List of Translatable and Hideable Lines

# In[167]:

hideable = [ "putexcel" ]
translatable = [ "local", "count", "replace" ]
translatable


# ### Kill Multiple Space Chars

# In[10]:

def kill_extra_spaces( raw_line ):
    
    # replace multiple spaces w/ just one
    return re.sub( ' +', ' ', raw_line )

kill_extra_spaces( "12  23  45               4 4 4 4 4        45 64 6  ASFD     " )


# ### Normalize Parenthesis

# In[133]:

def normalize_parenthesis( raw_line ):
    
    # add spaces between parens and chars
    raw_line = re.sub( r'\(', "( ", re.sub( r'\)', " )", raw_line ) )

    return kill_extra_spaces( raw_line )

normalize_parenthesis( "//         putexcel M21 = (r(N)),    sheet(       `report' ) foo(`bar')" )


# ### Normalize Plus Signs

# In[12]:

def normalize_plus_signs( raw_line ):
    
    # add spaces between plus signs and chars
    raw_line = re.sub( r'\+', " + ", raw_line )

    return kill_extra_spaces( raw_line )

normalize_plus_signs( "count if ( ( ( PIRL211+PIRL212+PIRL213+PIRL214+PIRL215 )==2 ) | ( ( PIRL211+PIRL212+PIRL213+PIRL214+PIRL215 )==3 ) | ( ( PIRL211+PIRL212+PIRL213+PIRL214+PIRL215 )==4 ) | ( ( PIRL211+PIRL212+PIRL213+PIRL214+PIRL215 )==5 ) ) & `FS' & `total' & `prevpartdef'" )


# ### Normalize Equals Signs

# In[14]:

def normalize_equals( raw_line ):
    
    # reduce and add spaces between equals signs and chars
    raw_line = re.sub( r'==', " = ", raw_line )

    return kill_extra_spaces( raw_line )

normalize_equals( "count if ( ( ( PIRL211 + PIRL212 + PIRL213 + PIRL214 + PIRL215 )==2 ) | ( ( PIRL211 + PIRL212 + PIRL213 + PIRL214 + PIRL215 )==3 ) | ( ( PIRL211 + PIRL212 + PIRL213 + PIRL214 + PIRL215 )==4 ) | ( ( PIRL211 + PIRL212 + PIRL213 + PIRL214 + PIRL215 )==5 ) ) & `FS' & `total' & `prevpartdef'" )


# ### Translate Conjunctions

# In[15]:

def translate_conjuntions( raw_line ):
    
    # replace | with OR, add spaces between
    raw_line = re.sub( r'\|', " OR ", raw_line )
    
    # replace & with AND, add spaces between
    raw_line = re.sub( r'&', " AND ", raw_line )
    
    return kill_extra_spaces( raw_line )

translate_conjuntions( "replace age = age - 1 if (month(PIRL906) < month(PIRL200)) | (month(PIRL906) == month(PIRL200) & day(PIRL906) < day(PIRL200))" )
    


# ### Translate Variable Declarations

# In[16]:

def translate_variable_declarations( raw_line ):
    
    raw_line = re.sub( '^local ([A-Za-z0-9]{1,}) "', r'val \1 = "', raw_line )
    
    return kill_extra_spaces( raw_line )

translate_variable_declarations( 'local Adult "(inlist(PIRL903,1,2,3,4))"' )


# ### Translate 'IN' clauses

# In[129]:

def translate_in_clause( raw_line ):
    
    # WORKS: raw_line = re.sub( r'(PIRL[0-9]{1,3})', r'__1st__\1__1st__', raw_line )
    # WORKS: raw_line = re.sub( r'inlist\(', 'MATCH', raw_line )
    # WORKS: raw_line = re.sub( r'(inlist)', r'--YEP--\1--YEP--', raw_line )
    # WO$RKS: raw_line = re.sub( r'(inlist\() (PIRL[0-9]{1,3},)', r'1st:[\1] 2nd: [\2]', raw_line )
    raw_line = re.sub( r'(inlist\() (PIRL[0-9]{1,})', r'\2 IN (', raw_line )
    
    # TODO: call method that strips first char (",") from match string, instead of this kludgey workaround
    # SEE: Date Variable Declarations below for example
    # kludgey, but it works: replace " IN (," with " IN ( "
    raw_line = re.sub( r' IN \(,', r' IN ( ', raw_line )
    
    return kill_extra_spaces( raw_line )

translate_in_clause( "( inlist( PIRL903,1,2,3,4 ) )" )    


# ### Translate Date Variable Declarations

# In[19]:

# declare month abbreviations to numerics as a dictionary
months = {}
months[ "jan" ] = "01"
months[ "feb" ] = "02"
months[ "mar" ] = "03"
months[ "apr" ] = "04"
months[ "may" ] = "05"
months[ "jun" ] = "06"
months[ "jul" ] = "07"
months[ "aug" ] = "08"
months[ "sep" ] = "09"
months[ "oct" ] = "10"
months[ "nov" ] = "11"
months[ "dec" ] = "12"

def substitute_months( match ):
    
    return 'val {} = "{}-{}-{}"'.format( match.group( 1 ), match.group( 4 ), months[ match.group( 3 ) ], match.group( 2 ) )

def translate_date_vars( raw_line ):
    
    # This line calls a function w/ the match object.  ¡Genial!
    # Here: https://stackoverflow.com/questions/17136127/calling-a-function-on-captured-group-in-re-sub
    raw_line = re.sub( r'local ([a-zA-Z0-9]{1,}) (\d\d)([a-zA-Z]{3})(\d\d\d\d)', substitute_months, raw_line )
    
    # Plain, w/o month abbreviation to numeric lookup
    #raw_line = re.sub( r'local ([a-zA-Z]{1,}) (\d\d)([a-zA-Z]{3})(\d\d\d\d)', r'var \1 = "\4-\3-\2"', raw_line )
    
    return kill_extra_spaces( raw_line )

translate_date_vars( "local begpart2date 01oct2017" )


# ### Translate "d( var )" to "to_date( '$date_var' )"

# In[20]:

def translate_date_function( raw_line ):
    
    raw_line = re.sub( r" d\( `([A-Za-z0-9]{1,})' \) ", r" to_date( '$\1' ) ", raw_line )
    
    return kill_extra_spaces( raw_line )

translate_date_function( "( PIRL900 <= d( `qtrend' ) )" )


# ### Translate Inline Variables

# In[28]:

def translate_inline_variable_insertions( raw_line ):
    
    raw_line = re.sub( r"\`([a-zA-Z0-9]{1,})'", r"$\1", raw_line )
                      
    return kill_extra_spaces( raw_line )

translate_inline_variable_insertions( "( `partsplit' AND ( PIRL900 <= to_date( '$qtrend' ) ) AND ( ( PIRL901 >= to_date( '$begpartdate' ) ) OR missing( PIRL901 ) ) )" )


# ### Remove References to "$FS AND "

# In[31]:

def remove_ref_to_fs( raw_line ):
    
    raw_line = re.sub( "\$FS AND ", "", raw_line )
    
    return kill_extra_spaces( raw_line )

remove_ref_to_fs( "count if $FS AND $bcsvc AND $exitdef" )


# ### Create Stop List of _Proper_ Date Columns

# In[44]:

proper_dates = [
    "DateOfProgramEntryWIOA",
    "DateOfProgramExitWIOA",
    "DateOfMostRecentMeasurableSkillGainsSkillsProgress",
    "DateOfMostRecentMeasurableSkillGainsTrainingMilest",
    "DateOfMostRecentMeasurableSkillGainsPostsecondaryT",
    "DateOfMostRecentMeasurableSkillGainsSecondaryTrans",
    "DateOfMostRecentMeasurableSkillGainsEducationalFun",
    "DateEnrolledInPostExitEducationOrTrainingProgramLe",
    "MostRecentDateReceivedBasicCareerServicesSelfServi",
    "DateOfMostRecentReportableIndividualContact",
    "DateAttainedRecognizedCredentialWIOA",
    "DateAttainedRecognizedCredential2",
    "DateAttainedRecognizedCredential3",
    "DateOfFirstWIOAYouthService",
    "CoveredPersonEntryDate",
    "DateOfBirthWIOA",
    "DateOfFirstIndividualizedCareerService",
    "DateOfFirstBasicCareerServiceSelfService1",
    "DateCompletedDuringProgramParticipationAnEducation",
    "DateEnrolledDuringProgramParticipationInAnEducatio"
]    

def is_proper_date( pirl_name ):
    
    parquet_name = pirl_dictionary[ pirl_name ]
    
    return parquet_name in proper_dates

print( is_proper_date( "PIRL900" ) ) # True
print( is_proper_date( "PIRL100" ) ) # False


# ### Translate !missing(...) According to Date Type: String vs Proper Date Column

# In[77]:

def get_not_missing_clause( match, debugging=False ):
    
    """String or proper date?  Returns properly formatted SQL that queries for '!missing(...)'"""
    
    if  debugging:
        
        print( match.group( 1 ) )
        print( match.group( 2 ) )
        print( is_proper_date( match.group( 2 ) ) )
    
    if is_proper_date( match.group( 2 ) ):
        
        return "{} IS NOT NULL".format( match.group( 2 ) )
    
    else:
        
        return "length( {} ) > 0 AND {} != '$uNull'".format( match.group( 2 ), match.group( 2 ) )
    
def translate_not_missing( raw_line ):
    
    raw_line = re.sub( r'(!missing\() (PIRL[0-9]{1,}) \)', get_not_missing_clause, raw_line )
        
    return kill_extra_spaces( raw_line )

print( translate_not_missing( "( !missing( PIRL1001 ) AND missing( PIRL1200 ) AND PIRL1300 != 1 )" ) )


# In[92]:

def get_missing_clause( match ):
    
    """String or proper date?  Returns properly formatted SQL that queries for 'missing(...)'"""
    
    if  debugging:
        
        print( match.group( 1 ) )
        print( match.group( 2 ) )
        print( is_proper_date( match.group( 2 ) ) )
    
    if is_proper_date( match.group( 2 ) ):
        
        return "{} IS NULL".format( match.group( 2 ) )
    
    else:
        
        return "( length( {} ) = 0 OR {} != '$uNull' )".format( match.group( 2 ), match.group( 2 ) )
    
def translate_missing( raw_line ):
    
    raw_line = re.sub( r'(missing\() (PIRL[0-9]{1,}) \)', get_missing_clause, raw_line )
        
    return kill_extra_spaces( raw_line )

print( translate_missing( "( length( PIRL1001 ) > 0 AND PIRL1001 != '$uNull' AND missing( PIRL1200 ) AND PIRL1300 != 1 )" ) )


# ### Translate PIRL to Parquet Names

# In[96]:

def get_column_name( match ):
    
    #return "BINGO!"
    return pirl_dictionary[ match.group( 1 ) ]

def translate_pirl_to_parquet( raw_line ):
    
    return re.sub( r'(PIRL[0-9]{1,})', get_column_name, raw_line )
        
print( translate_pirl_to_parquet( "( length( PIRL1001 ) > 0 AND PIRL1001 != '$uNull' AND missing( PIRL1200 ) AND PIRL1300 != 1 )" ) )


# ### Add Scala String Generation Prefix/Suffix

# In[163]:

def add_string_generation( raw_line ):
    
    """Prepends scala's string generation prefix if a ref to a $variable is found"""
    
    # any string gen in effect?
    if "$" in raw_line:        
        
        # insert s""" between variable name and string...
        raw_line = re.sub( r'(val [A-Za-z0-9]{1,} =) (")', r'\1 s"""', raw_line )
        # ...append '""".stripMargin' to end of string
        return re.sub( r'([\"]{1,}$)', r' """', raw_line.strip() )
        
    else:

        return raw_line

add_string_generation( """val foo = "( $partsplit AND ( DateOfProgramEntryWIOA <= to_date( '$qtrend' ) ) AND ( ( DateOfProgramExitWIOA >= to_date( '$begpartdate' ) ) OR DateOfProgramExitWIOA IS NULL ) )" """ )


# ### Translate "Count if" into SQL SELECT ...

# In[160]:

query_string_id = 0


# In[169]:

def get_unique_var_name( match ):
    
    return 'val query{} = s"""SELECT count( UniqueIndividualIdentifierWIOA ) as count FROM edrvs WHERE{} """'.format( query_string_id, match.group( 2 ) )

def translate_get_if( raw_line ):
    
    return re.sub( r'(count if)(.*)$', get_unique_var_name, raw_line )

translate_get_if( "count if $bcsvc AND $exitdef" )


# ### Wrap All Transformations in One Method

# In[142]:

def apply_transformations( line ):
    
    line = normalize_parenthesis( line )
    line = normalize_plus_signs( line )
    line = normalize_equals( line )
    line = translate_conjuntions( line )
    line = translate_variable_declarations( line )
    line = translate_in_clause( line )
    line = translate_date_vars( line )
    line = translate_date_function( line )
    line = translate_inline_variable_insertions( line )
    line = remove_ref_to_fs( line )
    # Not missing needs to be run before missing, due to weak(er) regex boundaries
    line = translate_not_missing( line )
    line = translate_missing( line )
    line = translate_pirl_to_parquet( line )
    line = add_string_generation( line )
    line = translate_get_if( line )
    
    return line


# ### Test, One Line at a Time

# In[132]:

apply_transformations( """local credden "(`partsplit' & !missing(PIRL900) & PIRL901 >= d(`beginq4date') & PIRL901 <= d(`endq4date') & ((inlist(PIRL1303,2,3,4,6,7,8,9,10) | inlist(PIRL1310,2,3,4,6,7,8,9,10) | inlist(PIRL1315,2,3,4,6,7,8,9,10)) | PIRL1332 == 1 | (PIRL408 == 0 & PIRL1401 == 1)) & PIRL923 == 0)"  """)


# ### Iterate Lines, Apply All Transformations

# In[170]:

debugging = False

counter = 1

for line in stata_list:
    
    words = line.split( " " )
    if words[ 0 ] in translatable:
        
        # update query id
        query_string_id = counter
        line = apply_transformations( line )
        
        print( "")
        print( line )
        
    elif words[ 0 ] not in hideable:
        
        print( "// " + line )
        
    counter += 1


# In[22]:

foo = stata_list[ 0 ]
print( foo )


# In[ ]:



