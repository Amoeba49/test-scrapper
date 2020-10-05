import pickle
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from tabulate import tabulate
import time

test_num = 0
division = ''
event = ''
sql_string = ''

b_events = ['Anatomy and Physiology',
        'Circuit Lab',
        'Crime Busters',
        'Density Lab',
        'Disease Detectives',
        'Dynamic Planet',
        'Experimental Design',
        'Food Science',
        'Fossils',
        'Heredity',
        'Machines',
        'Meteorology',
        'Ornithology',
        'Reach for the Stars',
        'Road Scholar',
        'Water Quality']

c_events = [
        'Anatomy & Physiology',
        'Astronomy',
        'Chemistry Lab',
        'Circuit Lab',
        'Codebusters',
        'Designer Genes',
        'Disease Detectives',
        'Dynamic Planet',
        'Experimental Design',
        'Fermi',
        'Forensics',
        'Food Science',
        'Fossils',
        'Geologic Mapping',
        'Machines',
        'Ornithology',
        'Protein Modeling',
        'Sounds of Music',
        'Water Quality'
    ]

event_years = [
        '2000',
        '2001',
        '2002',
        '2003',
        '2004',
        '2005',
        '2006',
        '2007',
        '2008',
        '2009',
        '2010',
        '2011',
        '2012',
        '2013',
        '2014',
        '2015',
        '2016',
        '2017',
        '2018',
        '2019',
        '2020',
        '2021',
        '2022'
    ]

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

def get_gdrive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # return Google Drive API service
    return build('drive', 'v3', credentials=creds)

def find_events(searchDivision='C'):
    global division
    global event
    global test_num
    events = []
    event_names = []
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 5 files the user has access to.
    """
    service = get_gdrive_service()
    # Call the Drive v3 API
    if(searchDivision == 'C'):
        division = 'C'
        parentId = '1eXLptD0kGRhULDpIOOK-qCU91ENC6_Dy' #C test library drive
    elif(searchDivision == 'B'):
        division = 'B'
        parentId= '1M48QGJ5VgKoKX9rp2ROe7zUOyhdD4Edf' #B test library drive
    results = service.files().list(
        pageSize=1000, fields="nextPageToken, files(id, name, parents, mimeType)", includeItemsFromAllDrives=True, corpora='user', supportsAllDrives=True, orderBy='name', q=f"parents='{parentId}'").execute()
    # get the results
    items = results.get('files', [])
    # search for items
    if not items:
        # empty drive
        print('No files found.')
    else:
        for item in items:
            # get the File ID
            id = item["id"]
            mime_type = item["mimeType"]   
            # get the name of file
            name = item["name"]
            if(mime_type == 'application/vnd.google-apps.folder' and (name in b_events or name in c_events)): #Look for folders with event names
                events.append(id)
                event_names.append(name)
            else:
                pass
        for parentId in events:    
            event = event_names[events.index(parentId)]
            print('\n' + event)
            service = get_gdrive_service()
            # Call the Drive v3 API
            results = service.files().list(
                pageSize=1000, fields="nextPageToken, files(name)", includeItemsFromAllDrives=True, corpora='user', supportsAllDrives=True, orderBy='name', q=f"parents='{parentId}'").execute()
            # get the results
            items = results.get('files', [])
            # list all 20 files & folders
            if not items:
                # empty drive
                print('No files found.')
            else:
                rows = []
                year_competitions = []
                # get the File ID
                for item in items:
                    # get the name of file
                    name = item["name"]
                    if(name.startswith('Anatomy and Physiology') or name.startswith('Disease Detectives') or name.startswith('Dynamic Planet') or name.startswith('Meteorology') or name.startswith('Reach for the Stars') or name.startswith('Solar System')):
                        if(len(name.split(' - ')) > 1):
                            name = name.split(' - ')
                            event = name[0]
                            year = name[2]
                            competition = name[3]
                            if(not(year.startswith('20'))):
                                year = name[1]
                                competition = name[2]
                            if(competition == 'None' or competition == ''):
                                continue
                            if((year + competition) in year_competitions):
                                continue
                            else:
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                        else:  
                            if(len(name.split('_')) > 1):
                                name = name.split('_')
                                event = name[0] + ' Flagged'
                                competition = name[3]
                                year = name[2]
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                            elif(len(name.split('_')) == 2):
                                name = name.split('_')
                                event = name[0]
                                competition = 'Nationals'
                                year = name[1] + ' Flagged'
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                            else:
                                event = '' 
                                competition = name
                                year = 'Flagged'
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                    else:    
                        if(len(name.split(' - ')) > 2):
                            name = name.split(' - ')
                            event = name[0]
                            year = name[1]
                            competition = name[2]
                            if(competition == 'None' or competition == ''):
                                continue
                            if(not(year + competition) in year_competitions):
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                        else:
                            if(len(name.split('_')) > 3):
                                name = name.split('_')
                                event = name[0] + ' Flagged'
                                competition = name[3]
                                year = name[2]
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                            elif(len(name.split('_')) == 2):
                                name = name.split('_')
                                event = name[0]
                                competition = 'Nationals'
                                year = name[1] + ' Flagged'
                                year_competitions.append(year + competition)
                                rows.append((event, year, competition))
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                            else:
                                event = '' 
                                competition = name
                                year = 'Flagged'
                                print(f"INSERT INTO Tests ('Division', 'TestEvent', 'TestYear', 'Competition', 'TestStatus', 'Students')\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                add_sql(f"INSERT INTO Tests (Division, TestEvent, TestYear, Competition, TestStatus, Students)\nVALUES ('{division}', '{event}', '{year}', '{competition}', '0', NULL);\n\n")
                                test_num += 1
                add_sql('/* Total number of tests: ' + str(test_num) + ' */\n')
        add_sql("DELETE FROM Tests\nWHERE Testid IN (SELECT * \n    FROM (SELECT Testid FROM Tests\n        GROUP BY `Division`, `TestEvent`, `TestYear`, `Competition` HAVING (COUNT(*) > 1) \n        ) AS A\n    )")


def add_sql(sql):
    global sql_string
    sql_string += sql
    f = open("SQL.sql", "w")
    f.write(sql_string)
    f.close()



if __name__ == '__main__':
    print('Starting new search')
    find_events('B')

'INSERT INTO Tests (Division, Event, Year, Competition, Status, Students)'
'VALUES (division, Event, Year, Competition, FALSE, NULL)'