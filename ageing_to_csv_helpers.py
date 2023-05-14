import pandas as pd



def getRunDate(finalDataFrame):
    runDate = finalDataFrame.at[0, 'C']
    return runDate


def getAdvisorId(finalDataFrame):
    advisorId: float = finalDataFrame.at[0, 'K']
    return str(int(advisorId))


def getAdvisorName(finalDataFrame):
    advisorName: str = finalDataFrame.at[2, 'B']
    return advisorName


def getClientStatus(finalDataFrame, fromIndex):
    clientStatus: str = finalDataFrame.at[fromIndex + 1, 'C']

    try:
        clientStatus = clientStatus.split(":")[1].strip()
    except:
        pass
    return clientStatus


def insertCsvHeader(csv_df):
    new_row = {'A': 'Run Date', 
        'B': 'Advisor ID', 
        'C': 'advisor', 
        'D': 'Account Number', 
        'E': 'Client Name', 
        'F': 'Client Status', 
        'G': 'Transaction Date', 
        'H': 'Transaction Details', 
        'I': 'Total', 
        'J': 'Current', 
        'K': '31 To 60 Days', 
        'L': '61 To 90', 
        'M': '91 To 120', 
        'N': 'Over 120 Days'}
    csv_df = csv_df.append(new_row, ignore_index=True)
    return csv_df