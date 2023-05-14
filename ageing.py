from re import A, L
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import timeit
import warnings
warnings.filterwarnings("ignore")


def getActiveColumnAndSum(_workDataFrame, sumColumns, previousActiveColumn):
    ### Get sum of column and add to dataframe
    ### sumColumns = ['F','G','H','I','J']
    columnToModifySumValue = -1

    if (previousActiveColumn != False):
        while(previousActiveColumn in sumColumns):  
            sumColumns.pop()
            # print('POPPED ACTIVE', sumColumns)

    ### Get column to update sum value
    for column in reversed(sumColumns):
        cellTotalSum = round(_workDataFrame[column].sum(), 2)
        if (cellTotalSum != 0):
            columnToModifySumValue = column
            break;
    
    ### If no columns is not zero
    if (columnToModifySumValue == -1):
        return -1, None, sumColumns
        
    return columnToModifySumValue, cellTotalSum, sumColumns 

def getLeftShiftColumn(_workDataFrame, sumColumns, previousActiveColumn):
    # print (sumColumns, '*** SUM COLUMNS ***', previousActiveColumn)
    columnToModifySumValue = -1
    if (previousActiveColumn != False):
        while(previousActiveColumn in sumColumns):  
            sumColumns.pop()

    ### Get column to update sum value
    for column in reversed(sumColumns):
        cellTotalSum = round(_workDataFrame[column].sum(), 2)
        if (cellTotalSum > 0):
            columnToModifySumValue = column
            break;
    
    return columnToModifySumValue

def dataFrameToList(_dataframe):
    try: 
        dataframeList = list(_dataframe.index.values)
        
        ### Remove duplicates
        dataframeList = list(dict.fromkeys(dataframeList))
    except:
        dataframeList = []
    return dataframeList

def getCreditNotesIndexValues(_workDataFrame):
    # print('************ RETRIEVING CREDIT NOTES INDEX VALUES ****************************')

    ### Get all entries where credit notes that can effect aging exist
    allCreditNotes = _workDataFrame.loc[(_workDataFrame['C'] =='CR') | (_workDataFrame['C'] =='IN') | (_workDataFrame['C'] =='DB')]
    
    ### Append filteredCreditNotesDF if credit note should be ignored
    filteredCreditNotes = pd.DataFrame()
    filteredInvoiceNotes = pd.DataFrame()
    filteredDBNotes = pd.DataFrame()

    for index, row in allCreditNotes.iterrows():
        duplicatesNotes = _workDataFrame.loc[(_workDataFrame['D'] == row.loc['D'])]
        allInvoice = duplicatesNotes.loc[(duplicatesNotes['C'] =='IN') | (_workDataFrame['C'] =='DB')]
        allDB = duplicatesNotes.loc[(_workDataFrame['C'] =='DB')]
        allCredit = duplicatesNotes.loc[(duplicatesNotes['C'] =='CR')]
        # print(allInvoice)
        # print(len(duplicatesNotes))
        # print(duplicatesNotes)
        if (len(allInvoice) > 0):            
            allInvoice = allInvoice.loc[(_workDataFrame['F'] != 0) |
                                        (_workDataFrame['G'] != 0) |
                                        (_workDataFrame['H'] != 0) |
                                        (_workDataFrame['I'] != 0) ]
            if (len(allInvoice) > 0):
                # print('************ checkOutdatedCreditNote **************')
                # print("**************************************************")
                # print(allInvoice)
                # print(allCredit)
                
                ### aging will ignore the lines with these index values
                filteredCreditNotes = filteredCreditNotes.append(allCredit)
                filteredInvoiceNotes = filteredInvoiceNotes.append(allInvoice)
                filteredDBNotes = filteredDBNotes.append(allDB)

    filteredInvoiceNotes = dataFrameToList(filteredInvoiceNotes)
    filteredCreditNotes = dataFrameToList(filteredCreditNotes)
    filteredDBNotes = dataFrameToList(filteredDBNotes)
    
    NewInvoiceNotes = []
    NewInvoiceNotes = filteredInvoiceNotes + filteredCreditNotes
    
    # print(filteredCreditNotes)
    # print(NewInvoiceNotes)
    
    return filteredCreditNotes, NewInvoiceNotes, filteredDBNotes 

def handleCreditNote(_workDataFrame, NewInvoiceNotes):
    activeColumn = None
    invoiceDate = None
    shiftColumn = None
    creditDate = None

    for index, row in _workDataFrame.iterrows():
        if (index in NewInvoiceNotes):
            activeColumn = None
            invoiceDate = None
            shiftColumn = None
            creditDate = None

            duplicatesNotes = _workDataFrame.loc[(_workDataFrame['D'] == row.loc['D'])]
            duplicateslist = dataFrameToList(duplicatesNotes)
            
            invoices = duplicatesNotes.loc[(duplicatesNotes['C'] =='IN')]
            credit = duplicatesNotes.loc[(duplicatesNotes['C'] =='CR')]

            # print('%%%%%%%%%%% START %%%%%%%%%%%')
            # print(invoices)
            # print(credit)

            ### Get column of invoice value
            for invoiceIndex, invoicesRow in invoices.iterrows():
                if (_workDataFrame.at[invoiceIndex, 'F'] != 0):
                    activeColumn = 'F'
                elif(_workDataFrame.at[invoiceIndex, 'G'] != 0):
                    activeColumn = 'G'
                elif(_workDataFrame.at[invoiceIndex, 'H'] != 0):
                    activeColumn = 'H'
                elif(_workDataFrame.at[invoiceIndex, 'I'] != 0):
                    activeColumn = 'I'
                
                if (activeColumn != None):
                    invoiceDate = (_workDataFrame.at[invoiceIndex, 'B'])
                    # print ("Invoice Index", invoiceIndex)
                    # print ("Invoice Date", invoiceDate)
                    break

            ### move credit to invoice column if required
            if (activeColumn != None):
                for creditIndex, creditRow in credit.iterrows():
                    if (_workDataFrame.at[creditIndex, 'F'] != 0):
                        shiftColumn = 'F'
                    elif(_workDataFrame.at[creditIndex, 'G'] != 0):
                        shiftColumn = 'G'
                    elif(_workDataFrame.at[creditIndex, 'H'] != 0):
                        shiftColumn = 'H'
                    elif(_workDataFrame.at[creditIndex, 'I'] != 0):
                        shiftColumn = 'I'

                    if (shiftColumn != None):
                        # print('********************')
                        # print(invoiceDate)
                        # print(invoiceDate.month)
                        # print(activeColumn)
                        # print(shiftColumn)
                        
                        if (activeColumn != shiftColumn):
                            # print ('CREDITNOTE MOVE COLUMN DATA 0')
                            _workDataFrame.at[creditIndex, activeColumn] = round(_workDataFrame.at[creditIndex, shiftColumn], 2)
                            _workDataFrame.at[creditIndex, shiftColumn] = 0
                    
            ### Remove all duplicates from loops
            NewInvoiceNotes = set(NewInvoiceNotes) - set(duplicateslist)
    
    return _workDataFrame

def getRowValue(_workDataFrame, index):
    columnF = _workDataFrame.loc[index, 'F']
    columnG = _workDataFrame.loc[index, 'G']
    columnH = _workDataFrame.loc[index, 'H']
    columnI = _workDataFrame.loc[index, 'I']
    columnJ = _workDataFrame.loc[index, 'J']

    if (columnF != 0):
        return 'F', columnF

    elif (columnG != 0):
        return 'G', columnG

    elif (columnH != 0):
        return 'H', columnH
    
    elif (columnI != 0):
        return 'I', columnI

    elif (columnJ != 0):
        return 'J', columnJ
    else:
        return None, None


def handleDbNote(_workDataFrame, NewDbNotes):
    sameColumn = []
    handled = []

    # print("HandleDbNote START ******************************")
    for index in NewDbNotes:
        activeColumn = None
        shiftColumn = None

        accNumber = _workDataFrame.loc[index, 'D']
        accNumbersIndex = _workDataFrame.index[_workDataFrame['D'] == accNumber].tolist()

        activeColumn, activeColumnValue = getRowValue(_workDataFrame, index)
 
        for accIndex in reversed(accNumbersIndex):
            shiftColumn, shiftColumnValue = getRowValue(_workDataFrame, accIndex)

            if (-activeColumnValue == shiftColumnValue):
                if(shiftColumn == activeColumn):
                    sameColumn.append(index)
                    sameColumn.append(accIndex)

                if(shiftColumn > activeColumn):
                    # If in same column do not move values

                    if (index not in sameColumn) or (accIndex not in sameColumn): 
                        if (index not in handled):
                            if (accIndex not in handled): 
                                # print("SAME COLUMN FOUND IN")
                                # print(index, activeColumn, activeColumnValue, accIndex, shiftColumn, shiftColumnValue)
                                _workDataFrame.at[index, shiftColumn] = activeColumnValue
                                _workDataFrame.at[index, activeColumn] = 0
                                # print(index, activeColumn, activeColumnValue, accIndex, shiftColumn, shiftColumnValue)

                                handled.append(index)
                                handled.append(accIndex)
                                # print(handled)
                    

                # if(shiftColumn > activeColumn):
                #     print("SAME VALUES")


    # print("HandleDbNote END ******************************")
    return _workDataFrame



### Check for Credit and Debit notes with the same name and amount
### This Credit is placed in a list to be filtered out so that it cannot be used in the aging process.
def RemoveUsedCreditDebit(_workDataFrame):
    filteredFoundCredit = pd.DataFrame()
    allOld = _workDataFrame.loc[(_workDataFrame['F'] == 0) |
                                (_workDataFrame['G'] == 0) |
                                (_workDataFrame['H'] == 0) |
                                (_workDataFrame['I'] == 0) ]


    debit = allOld.loc[(allOld['C'] =='DB')]
    credit = allOld.loc[(allOld['C'] =='CR')]

    debitReverse = debit[::-1]
    creditReverse = credit[::-1]
    index = -1


    for debitIndex, debitRow in debitReverse.iterrows():
        for creditIndex, creditRow in creditReverse.iterrows():
            # Check that names are the same
            if(debitRow.at['D'] == creditRow.at['D']):
                # Check that names are the same
                if(debitRow.at['J'] == -creditRow.at['J']):
                    filteredFoundCredit = filteredFoundCredit.append(creditRow)
                    index = creditIndex
                    break

        if (index != -1):
            creditReverse.drop(index, inplace=True)
            index = -1

    return dataFrameToList(filteredFoundCredit)




def preAging(_originalDataFrame, _fromIndex, _toIndex):
    ### Read original report total values and determine if aging is required
    workDataFrame = _originalDataFrame.loc[_fromIndex + 2:_toIndex]
    
    ### Fix date Format
    workDataFrame = fixDates(workDataFrame)
    
    ### Check that columns E, F, G, H, I, J is 0
    cellE = workDataFrame.loc[_toIndex, 'E']
    cellF = workDataFrame.loc[_toIndex, 'F']
    cellG = workDataFrame.loc[_toIndex, 'G']
    cellH = workDataFrame.loc[_toIndex, 'H']
    cellJ = workDataFrame.loc[_toIndex, 'J']

    ### Skip data if all values is true
    ### No Need to check data all totals are 0
    if ((cellE == 0) and (cellF == 0) and (cellG == 0) and (cellH == 0) and (cellJ == 0)):
        workDataFrame = _originalDataFrame.loc[_fromIndex + 1:_toIndex]
        isDataModified = False
        return workDataFrame, isDataModified
    else:
        ### Get only required block
        workDataFrame = _originalDataFrame.loc[_fromIndex + 2:_toIndex - 1]

        removeUsedCreditDebit = RemoveUsedCreditDebit(workDataFrame)
    
        ### Get Credit notes that require aging
        filteredCreditNotes, NewInvoiceNotes, filteredDBNotes = getCreditNotesIndexValues(workDataFrame)
        filteredCreditNotes.extend(removeUsedCreditDebit)
        # print(filteredCreditNotes)

        ## Move Invoices and credit notes to balance
        if (len(NewInvoiceNotes) != 0):
            workDataFrame = handleCreditNote(workDataFrame, NewInvoiceNotes)

        # Move DB to right column if a credit exist
        if (len(filteredDBNotes) != 0):
            workDataFrame = handleDbNote(workDataFrame, filteredDBNotes)

        ### Age the data
        workDataFrame = startAging(workDataFrame, filteredCreditNotes)
 
        ### SUM HERE
        ### Reinsert last row (Footer)
        ### Get sum of column and add to dataframe
        sumColumns = ['E','F','G','H','I','J']
        
        for columnName in sumColumns:
            workDataFrame.at[_toIndex, columnName] = round(workDataFrame[columnName].sum(), 2)
        
        isDataModified = True
        return workDataFrame, isDataModified

def startAging(_workDataFrame, _filteredCreditNotes):
    sumColumns = ['F','G','H','I','J']
    ActiveColumn = False

    while(-1):
        
        ### Get column that requires to be aged
        ActiveColumn, ActiveCellSum, sumColumns = getActiveColumnAndSum(_workDataFrame, sumColumns, ActiveColumn)
        
        if (ActiveColumn == -1):            
            return _workDataFrame

        ### Get column that values can be moved
        shiftColumn = chr(ord(ActiveColumn)-1)
        # print('****** MAIN ACTIVE LOOP', ActiveColumn, shiftColumn, ActiveCellSum)

        negativeValues = []
        while(shiftColumn != 'E'):
            negativeValues = _workDataFrame.loc[_workDataFrame[shiftColumn] < 0]

            # Remove rows that need to be ignored and not aged
            negativeValues.drop(_filteredCreditNotes, inplace=True, errors='ignore')
            
            # print(negativeValues)
            # print(_filteredCreditNotes)
        
            # print ('##################################################################')
            # print('BEFORE SECOND LOOP', ActiveColumn, shiftColumn, ActiveCellSum)
            # print(negativeValues)
            # print(_filteredCreditNotes)

            ### Only negative values will in the shiftColumn must be moved
            if (len(negativeValues) != 0):
                for IndexValue in reversed(list(negativeValues.index.values)):
                    ### Check that shift won't cause ActiveCell total to be less than 0
                    if ((round(_workDataFrame.at[IndexValue, shiftColumn] + ActiveCellSum, 2)) > 0):
                        ### Shift data from shift column to activeColumn
                        # print(round(_workDataFrame[ActiveColumn].sum(), 2), ActiveColumn, 'FIRST IF BEFORE', _workDataFrame.at[IndexValue, shiftColumn]) 
                        _workDataFrame.at[IndexValue, ActiveColumn] = round(_workDataFrame.at[IndexValue, shiftColumn], 2)
                        _workDataFrame.at[IndexValue, shiftColumn] = 0
                        # print(round(_workDataFrame[ActiveColumn].sum(), 2), ActiveColumn, 'FIRST IF AFTER')  
                    elif (ActiveCellSum > 0):
                        ### Make ActiveColumn Zero by deducting the correct amount form shiftColumn
                        # print(round(_workDataFrame[ActiveColumn].sum(), 2), ActiveColumn, _workDataFrame.at[IndexValue, shiftColumn], 'SECOND IF BEFORE')
                        _workDataFrame.at[IndexValue, ActiveColumn] = round(-ActiveCellSum, 2)
                        _workDataFrame.at[IndexValue, shiftColumn] = round(_workDataFrame.at[IndexValue, shiftColumn] + ActiveCellSum, 2)
                        # print(round(_workDataFrame[ActiveColumn].sum(), 2), ActiveColumn, 'SECOND IF AFTER', _workDataFrame.at[IndexValue, shiftColumn])
                    
                    ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)
                    # print("BEFORE BREAK", ActiveCellSum, ActiveColumn)
                    if (ActiveCellSum == 0):
                        # print('BREAK')
                        break
                    
            ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)
            
            ### SHIFT NEGATIVE VALUES TO THE LEFT  
            ### Will only happen if right shifting is done
            if (ActiveCellSum < 0 ):    

                ### Check that the negative values can be moved
                leftShiftColumn = getLeftShiftColumn(_workDataFrame, sumColumns, ActiveColumn)

                while ((leftShiftColumn != -1) or (ActiveCellSum == 0) or (leftShiftColumn == ActiveCellSum)):                                    
                    negativeActiveCellValues = _workDataFrame.loc[_workDataFrame[ActiveColumn] < 0]

                    # Remove rows that need to be ignored and not aged
                    negativeActiveCellValues.drop(_filteredCreditNotes, inplace=True, errors='ignore')
                    # print("********************************************************************************************")
                    # print(negativeActiveCellValues)
                    # print(_filteredCreditNotes)
                    # print("********************************************************************************************")

                    leftShiftColumn = getLeftShiftColumn(_workDataFrame, sumColumns, ActiveColumn)
                    ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)

                    if (ActiveColumn == leftShiftColumn):
                        # print(leftShiftColumn, ActiveColumn, ActiveCellSum, 'BREAK LEFTSHIFT LOOP 1')
                        break

                    if (leftShiftColumn == -1):
                        # print(leftShiftColumn, ActiveColumn, ActiveCellSum, 'BREAK LEFTSHIFT LOOP 2')
                        break


                    if (ActiveCellSum == 0):
                        # print(leftShiftColumn, ActiveColumn, ActiveCellSum, 'BREAK LEFTSHIFT LOOP 3')
                        break
                    
                    for IndexValue in reversed(list(negativeActiveCellValues.index.values)):
                        if(leftShiftColumn != -1):
                            if((ActiveCellSum + round(_workDataFrame[leftShiftColumn].sum(), 2)) > 0):
                                # print('LEFT SHIFT 3   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                activeTemp = round(_workDataFrame.at[IndexValue, ActiveColumn], 2)

                                if (ActiveCellSum < activeTemp):
                                    # print('LEFT SHIFT 3A   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = activeTemp
                                    _workDataFrame.at[IndexValue, ActiveColumn] = 0
                                else:
                                    # print('LEFT SHIFT 3B   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    activeTemp = (round(_workDataFrame.at[IndexValue, ActiveColumn], 2) - ActiveCellSum)
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = (round(_workDataFrame[ActiveColumn].sum(), 2))
                                    _workDataFrame.at[IndexValue, ActiveColumn] = activeTemp                                
                                    ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)

                                ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)
                                # print('LEFT SHIFT 3   AFTER  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)


                            elif((ActiveCellSum + round(_workDataFrame[leftShiftColumn].sum(), 2)) < 0):
                                # print(_workDataFrame.to_string())
                                # print('LEFT SHIFT 4   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn, round(_workDataFrame.at[IndexValue, ActiveColumn], 2))

                                LeftSumTemp = round(_workDataFrame[leftShiftColumn].sum(), 2)
                                activeTemp = round(_workDataFrame.at[IndexValue, ActiveColumn], 2)

                                if ((activeTemp + LeftSumTemp) > 0):
                                    # print('LEFT SHIFT 4A   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    _workDataFrame.at[IndexValue, ActiveColumn] = 0
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = activeTemp
      
                                else:
                                    # print('LEFT SHIFT 4B   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    activeTemp = _workDataFrame.at[IndexValue, ActiveColumn] + (round(_workDataFrame[leftShiftColumn].sum(), 2))
                                    leftShiftTemp = -(round(_workDataFrame[leftShiftColumn].sum(), 2))
                                 
                                    _workDataFrame.at[IndexValue, ActiveColumn] = activeTemp
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = leftShiftTemp

                                ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)
                                # print('LEFT SHIFT 4   AFTER  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn, round(_workDataFrame.at[IndexValue, ActiveColumn], 2))
                                # print(_workDataFrame.to_string())


                            else:
                                ### THIS IS HIT VERY INFREQUENTLY
                                ### Will move values to the right (RIGHT SHIFT)
                                
                                # print('LEFT SHIFT 5   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn, round(_workDataFrame.at[IndexValue, ActiveColumn], 2))
                                activeTemp = round(_workDataFrame.at[IndexValue, ActiveColumn], 2)
                                if (ActiveCellSum < activeTemp):
                                    # print('LEFT SHIFT 5A   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = activeTemp
                                    _workDataFrame.at[IndexValue, ActiveColumn] = 0
                                else:
                                    # print('LEFT SHIFT 5B   BEFORE  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn)
                                    activeTemp = (round(_workDataFrame.at[IndexValue, ActiveColumn], 2) - ActiveCellSum)
                                    _workDataFrame.at[IndexValue, leftShiftColumn] = (round(_workDataFrame[ActiveColumn].sum(), 2))
                                    _workDataFrame.at[IndexValue, ActiveColumn] = activeTemp                                
                                    ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)

                                # print('LEFT SHIFT 5   AFTER  **** ', ActiveCellSum, ActiveColumn, leftShiftColumn, round(_workDataFrame.at[IndexValue, ActiveColumn], 2))
                         
                        leftShiftColumn = getLeftShiftColumn(_workDataFrame, sumColumns, ActiveColumn)
                        ActiveCellSum = round(_workDataFrame[ActiveColumn].sum(), 2)
                         # print("BEFORE NEGATIVE LOOP BREAK", ActiveCellSum)
                    
                        if (ActiveCellSum == 0):
                            # print('NEGATIVE LOOP BREAK')
                            break

                    # print(negativeActiveCellValues.empty, ActiveCellSum, ActiveColumn)
                    
                    # Exit loop if there is no values exist to do ageing with
                    if (negativeActiveCellValues.empty == True):
                        # print(leftShiftColumn, ActiveColumn, ActiveCellSum, 'BREAK LEFTSHIFT LOOP 4')
                        break   
            
            # print("BEFORE BREAK", ActiveCellSum)
            if (ActiveCellSum == 0):
                # print('BREAK')
                break


            ### Switch to next column to move data, this must always happen last.
            shiftColumn = chr(ord(shiftColumn)-1)
            # print('AFTER SECOND LOOP', ActiveColumn, shiftColumn, ActiveCellSum)
            
    # print ('ERROR, LOOP MUST NOT GET TO THIS POINT')
    return _workDataFrame

def fixDates(_workDataFrame):
    _workDataFrame['B'] = pd.to_datetime(_workDataFrame['B']).dt.date
    return _workDataFrame

def negativeValuesCopyToTab(_dataFrame, _toIndex):
    ### Check for negativeSum Value of column 'E'
    if (_dataFrame.at[_toIndex,'E'] < 0):
        return True
    else:
        return False

def convertDateTimeToDate(cellValue):
    ### Remove time characters
    cellValue = str(cellValue)
    cellValue = cellValue[:-9]
    return (cellValue)

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


### Uncomment to test code without GUI

# Ageing = Ageing()
# Ageing.importFile("Empty - DeonB_600003579.xlsx")
# # start = timeit.default_timer()
# Ageing.modifyAndAppendHeader()
# Ageing.processAging()
# Ageing.processFinalTotal('final')
# Ageing.processFinalTotal('negative')
# # stop = timeit.default_timer()
# Ageing.printFinalDataFrameDataFrameToFile()
# # print('Time: ', stop - start)

