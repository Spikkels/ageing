from re import A, L
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import timeit
import warnings
warnings.filterwarnings("ignore")

from ageing_helpers import *
from ageing_to_csv_helpers import *


class Ageing:
    def __init__(self):
        self.fromIndex = 0
        self.toIndex = 0
        self.originalDataFrame = pd.DataFrame() 
        self.workDataFrame = pd.DataFrame()
        self.finalDataFrame = pd.DataFrame()
        self.NegativeValuesTab = pd.DataFrame()
        self.excel_file_path = ''
        self.excel_file_path_output = ''
        self.csv_df = pd.DataFrame(columns=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])

    def modifyAndAppendHeader(self, headerEndLine = 2):
        self.headerEndLine = headerEndLine
        
        ### Modify Values so that only Date is displayed and not date and time
        self.originalDataFrame.at[0, 'C'] = convertDateTimeToDate(self.originalDataFrame.at[0, 'C'])
        self.originalDataFrame.at[0, 'E'] = convertDateTimeToDate(self.originalDataFrame.at[0, 'E'])

        ### Modify Header
        self.originalDataFrame.loc[1] = ['','','','','Total','Current','31 To 60 Days','61 To 90 Days','91 To 120 Days','Over 120 Days','','','']
                
        ### Write Header to finalDataFrame and NegativeValuesTab
        self.finalDataFrame = self.finalDataFrame.append(self.originalDataFrame.loc[0:2])
        self.NegativeValuesTab = self.NegativeValuesTab.append(self.originalDataFrame.loc[0:2])

    def processAging(self):
        isDataModified = True
        
        ### Get list of index numbers where each new data block starts
        self.toIndex = self.originalDataFrame.loc[self.originalDataFrame['A'].isnull(), 'A'].index.tolist()

        ### Reverse list so that the first entries can be popped
        self.toIndex.reverse()
        fromIndex = self.headerEndLine
        toIndex = -1

        while (len(self.toIndex) != 2):
            toIndex = self.toIndex.pop()
            dataFrame, isDataModified = preAging(self.originalDataFrame, fromIndex, toIndex)
            if (isDataModified == True): 
                self.workDataFrame = self.workDataFrame.append(self.originalDataFrame.loc[fromIndex + 1])
                self.workDataFrame = self.workDataFrame.append(dataFrame)
                
                ### Insert all negative clients into another tab.
                copyDataSet = negativeValuesCopyToTab(dataFrame, toIndex)
                
                ### Insert data into new dataset if sum value is negative
                if (copyDataSet == True):
                    self.NegativeValuesTab = self.NegativeValuesTab.append(self.originalDataFrame.loc[fromIndex + 1])
                    self.NegativeValuesTab = self.NegativeValuesTab.append(dataFrame)                 
            else:
                self.workDataFrame = self.workDataFrame.append(dataFrame)
            fromIndex = toIndex
        
        self.finalDataFrame = self.finalDataFrame.append(self.workDataFrame)

    def processFinalTotal(self, useOnDataFrame):
        # print("FINAL", useOnDataFrame)
        if (useOnDataFrame == 'final'):
            dataFrame = self.finalDataFrame
        else:
            dataFrame = self.NegativeValuesTab

        tempDataFrame = pd.DataFrame()

        ### Get list of index numbers where each new data block starts
        self.toIndex = dataFrame.loc[dataFrame['A'].isnull(), 'A'].index.tolist()

        ### Reverse list so that the first entries can be popped
        self.toIndex.reverse()
        fromIndex = self.headerEndLine
        toIndex = -1
        
        # print('List of index of all totals')
        # print (self.toIndex)

        ### Get location of entries that will be processed in a block
        while (len(self.toIndex) != 0):
            toIndex = self.toIndex.pop()
            tempDataFrame = tempDataFrame.append(dataFrame.loc[toIndex])
            fromIndex = toIndex
        
        
        dataFrameRowIndex = len(dataFrame)
        # print('FINAL ROW INDEX 1: ',len(dataFrame))

        ### Get Original Total Value
        OriginalRowIndex = len(self.originalDataFrame)
        dataFrame = dataFrame.append(self.originalDataFrame.loc[OriginalRowIndex - 2]).reset_index(drop=True)
        
        ### Get Final Dataframe length
        dataFrameRowIndex = len(dataFrame)
        
        # print('FINAL ROW INDEX 2: ',len(dataFrame))
        # print(len(dataFrame))
        # print('BEFORE ********************************************')
        # print(dataFrame['E','F','G','H','I','J'])
        
        #Calculate final sum amount
        dataFrame.at[dataFrameRowIndex - 1, 'D'] = 'Original Total'
        dataFrame.at[dataFrameRowIndex, 'D'] = 'Aged Total'

        dataFrameRowIndex = len(dataFrame)
        # print('FINAL ROW INDEX 3: ',len(dataFrame))
        
        sumColumns = ['E','F','G','H','I','J']
        ### lastRow 
        try:
            for columnName in sumColumns:
                pass
                # print(columnName,'INSIDE BEFORE ********************************************')
                # print(round(tempDataFrame[columnName].sum(), 2), finalRowIndex)
                
                dataFrame.at[dataFrameRowIndex - 1, columnName] = round(tempDataFrame[columnName].sum(), 2)
                # print('columnName', columnName, 'finalRowIndex', finalRowIndex + 1, 'sum', round(tempDataFrame[columnName].sum(), 2), 'cell', dataFrame.at[finalRowIndex + 1, columnName])
                # print('INSIDE AFTER ********************************************')
        except:
            #Will happen when there is no negative values 
            pass
        # print(finalRowIndex + 1)
        # print(len(dataFrame) + 1)
        # print('OUTSIDE ********************************************')
        # print(dataFrame.loc)
        
        if (useOnDataFrame == 'final'):
            # print('F1 ********************************************')
            self.finalDataFrame = dataFrame 
        else:
            # print('F2 ********************************************')
            self.NegativeValuesTab = dataFrame 
        
    def importFile(self, _excelFilePath):
        ### Convert path to for any OS
        self.excel_file_path = Path(_excelFilePath)
        self.excel_file_path_output = Path(_excelFilePath + '_output.xlsx')
        
        ### Import excel file
        readExcelFile = pd.read_excel(self.excel_file_path, header=None)
        self.originalDataFrame = self.originalDataFrame.append(readExcelFile)
        self.originalDataFrame = self.originalDataFrame.rename(columns = {0 : 'A',
                                                                          1 : 'B',
                                                                          2 : 'C',
                                                                          3 : 'D',
                                                                          4 : 'E',
                                                                          5 : 'F',
                                                                          6 : 'G',
                                                                          7 : 'H',
                                                                          8 : 'I',
                                                                          9 : 'J',
                                                                          10 : 'K',
                                                                          11 : 'L',
                                                                          12 : 'M',
                                                                          }) 


    def printWorkDataFrameToFile(self):
        ### Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter("pandas_column_formats.xlsx", engine='xlsxwriter')

        ### Convert the dataframe to an XlsxWriter Excel object.
        self.workDataFrame.to_excel(writer, sheet_name='Sheet1', index = False, header= False)

        ### Get the xlsxwriter workbook and worksheet objects.
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']

        ### Add some cell formats.
        format1 = workbook.add_format({'num_format': '#,##0.00_);(#,##0.00)'})
        worksheet.set_column('E:J', None, format1)

        ### Close the Pandas Excel writer and output the Excel file.
        writer.save()


    def printFinalDataFrameDataFrameToFile(self):
        ### Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter((self.excel_file_path_output), engine='xlsxwriter')

        firstTabName = self.originalDataFrame.at[2, 'B']
        
        ### Convert the dataframe to an XlsxWriter Excel object.        
        self.finalDataFrame.to_excel(writer, sheet_name=firstTabName, index = False, header= False)
        self.NegativeValuesTab.to_excel(writer, sheet_name='ct', index = False, header= False)

        ### Get the xlsxwriter workbook and worksheet objects.
        workbook  = writer.book
        worksheet1 = writer.sheets[firstTabName]
        worksheet2 = writer.sheets['ct']
        
        ### Add some cell formats.
        format1 = workbook.add_format({'num_format': '#,##0.00_);(#,##0.00)'})
        worksheet1.set_column('E:J', None, format1)
        worksheet2.set_column('E:J', None, format1)
        
        ### Close the Pandas Excel writer and output the Excel file.
        format2 = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        worksheet1.set_column('B:C', None, format2)
        worksheet2.set_column('B:C', None, format2)

        writer.save()


    def ageingToCsvFormat(self):  
        self.csv_df = insertCsvHeader(self.csv_df)
        
        rows = []
        run_date = getRunDate(self.finalDataFrame)
        advisor_id = getAdvisorId(self.finalDataFrame)
        advisor_name = getAdvisorName(self.finalDataFrame)      

        ### Get list of index numbers where each new data block starts
        self.toIndex = self.finalDataFrame.loc[self.finalDataFrame['A'].isnull(), 'A'].index.tolist()
        print(self.toIndex)

        ### Reverse list so that the first entries can be popped
        self.toIndex.reverse()
        fromIndex = self.headerEndLine
        toIndex = -1

        while (len(self.toIndex) != 2):

            toIndex = self.toIndex.pop()
            print(toIndex, fromIndex)

            clientNumber = self.finalDataFrame.at[fromIndex + 1, 'A']
            clientName = self.finalDataFrame.at[fromIndex + 1, 'B']
            clientStatus = getClientStatus(self.finalDataFrame, fromIndex)

            print(clientNumber)
            print(clientName)
            print(clientStatus)

            workDataFrame = self.finalDataFrame.loc[(fromIndex) + 2:(toIndex- 1)]


            for index, row in workDataFrame.iterrows():

                transactionDate = row['B']
                transaction_details = row['D']
                total = row['E']
                current = row['F']
                age_31_to_60 = row['G']
                age_61_to_90 = row['H']
                age_91_to_120 = row['I']
                age_over_120 = row['J']

                new_row = {'A': run_date, 
                           'B': advisor_id, 
                           'C': advisor_name, 
                           'D': clientNumber, 
                           'E': clientName, 
                           'F': clientStatus, 
                           'G': transactionDate, 
                           'H': transaction_details, 
                           'I': total, 'J': current, 
                           'K': age_31_to_60, 
                           'L': age_61_to_90, 
                           'M': age_91_to_120, 
                           'N': age_over_120}
                rows.append(new_row)


            fromIndex = toIndex
        
        self.csv_df = self.csv_df.append(rows, ignore_index=True)
        print(self.csv_df)
        # self.finalDataFrame = self.finalDataFrame.append(self.workDataFrame)        


    def exportToCsv(self):
        self.finalDataFrame.to_csv(self.csv_file_path_output, index=False)


### Uncomment to test code without GUI

# Ageing = Ageing()
# Ageing.importFile("deon b aa 31jul22.xlsx")
# # start = timeit.default_timer()
# Ageing.modifyAndAppendHeader()
# Ageing.processAging()
# Ageing.processFinalTotal('final')
# Ageing.ageingToCsvFormat()
# # Ageing.processFinalTotal('negative')
# # stop = timeit.default_timer()
# # Ageing.printFinalDataFrameDataFrameToFile()
# # print('Time: ', stop - start)

