import sys
import PyQt5
from PyQt5 import QtCore, QtGui, uic, QtWidgets
from PyQt5.QtWidgets import QWidget
from PyQt5.QtCore import QObject, QThread, pyqtSignal
from PyQt5.QtWidgets import QApplication, QWidget, QInputDialog, QLineEdit, QFileDialog
from PyQt5.QtGui import QIcon
from pathlib import Path
import time

import ageing

# Enable high DPI scaling
if hasattr(QtCore.Qt, 'AA_EnableHighDpiScaling'):
    PyQt5.QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling, True)

if hasattr(QtCore.Qt, 'AA_UseHighDpiPixmaps'):
    PyQt5.QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        
        MainWindow.resize(431, 357)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.importButton = QtWidgets.QPushButton(self.centralwidget)
        self.importButton.setGeometry(QtCore.QRect(10, 10, 75, 23))
        self.importButton.setObjectName("importButton")
        self.runAgingButton = QtWidgets.QPushButton(self.centralwidget)
        self.runAgingButton.setGeometry(QtCore.QRect(10, 310, 111, 23))
        self.runAgingButton.setObjectName("runAgingButton")
        self.runAgingButton.setEnabled(False)
        self.tableWidget = QtWidgets.QTableWidget(self.centralwidget)
        self.tableWidget.setGeometry(QtCore.QRect(10, 40, 411, 261))
        self.tableWidget.setAlternatingRowColors(True)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(2)
        self.tableWidget.setColumnWidth(0, 250)
        self.tableWidget.setColumnWidth(1, 150)
        self.tableWidget.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, item)
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Debtors Aging"))
        self.importButton.setText(_translate("MainWindow", "Import File(s)"))
        self.runAgingButton.setText(_translate(
            "MainWindow", "Run Ageing Process"))
        item = self.tableWidget.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "File Name"))
        item = self.tableWidget.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "Completed Successfully"))
        self.importButton.clicked.connect(self.importButtonHandler)
        self.runAgingButton.clicked.connect(self.runAgingHandler)

    def runLongTask(self):
        # Step 2: Create a QThread object
        self.thread = QThread()
        # Step 3: Create a worker object
        self.worker = Worker(self.filenames)
        # self.worker.filePaths(self.filenames)
        # Step 4: Move worker to the thread
        self.worker.moveToThread(self.thread)
        # Step 5: Connect signals and slots
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.progress.connect(self.reportProgress)
        # Step 6: Start the thread
        self.thread.start()

        # Final resets
        self.runAgingButton.setEnabled(False)
        self.thread.finished.connect(lambda: self.runAgingButton.setEnabled(True))
        # self.thread.finished.connect(lambda: self.stepLabel.setText("Long-Running Step: 0"))

    #Receives update as a string and updates GUI with this data.
    def reportProgress(self, n):
        test = n.split(",")
        row = int(test[0])
        message = test[1]
        self.tableWidget.setItem(row, 1, QtWidgets.QTableWidgetItem(message))

    def importButtonHandler(self):
        dlg = QFileDialog()
        dlg.setFileMode(QFileDialog.ExistingFiles)
        dlg.setNameFilters(["Excel Files (*.xlsx)"])

        if dlg.exec_():
            self.filenames = dlg.selectedFiles()
            row = 0
            self.tableWidget.setRowCount(len(self.filenames))
            for filename in self.filenames:
                path = Path(filename)
                self.tableWidget.setItem(
                    row, 0, QtWidgets.QTableWidgetItem(path.stem))
                row = row + 1

            self.runAgingButton.setEnabled(True)

    def runAgingHandler(self):
        self.runLongTask()

#Worker Thread to prevent GUI from hanging
class Worker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal([str])
    
    def __init__(self, _filePaths, parent=None):
        QThread.__init__(self, parent)
        self.filePaths = _filePaths

    #RunAging
    def run(self):
        row = 0
        
        for filename in self.filePaths:            
            try:
                path = Path(filename)        
                ageing1 = ageing.Ageing()
                
                self.progress.emit(f'{row}, Importing File...')
                ageing1.importFile(filename)
                
                self.progress.emit(f'{row}, Aging...')
                ageing1.modifyAndAppendHeader()
                ageing1.processAging()
                ageing1.processFinalTotal('final')
                ageing1.processFinalTotal('negative')
                
                self.progress.emit(f'{row}, Exporting File...')
                ageing1.printFinalDataFrameDataFrameToFile()

                self.progress.emit(f'{row}, Completed')
            except:
                self.progress.emit(f'{row}, Failed')

            row = row + 1

        self.finished.emit()


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
