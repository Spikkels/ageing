1.  Running the source code

1.1 To Run code install Python 3.9.2

1.2 Create a python virtual enviroment and the required libraries by running the following commands
    Online Guide https://docs.python-guide.org/dev/virtualenvs/

1.2.1  pip install pipenv 

1.2.2  Create virtual environment

1.2.3  cd project_folder
       virtualenv venv

       ## Start using virtual environment
       source venv/bin/activate

1.2.4  ## Install libraries
       pip install -r requirements.txt

1.2.5  The code can now be run by typing
       py uitest.py

1.2.6  The aging.py library can be ran independantly by commenting in code (Bottom of the code page in ageing.py 
       and choosing a test file in the code

2.  Convert .py into .exe  (Online Guide https://pypi.org/project/auto-py-to-exe/)
2.1  In the Windows terminal in your project directory auto-py-to-exe this will open a GUI to do the conversion.

2.2  Choosing the correct options the auto-py-to-exe

2.2.1  Script location choose uitest.py
2.2.2  Choose one File
2.2.3  Choose Window based (hide the console)
2.2.3  Press Convert .PY to .EXE 
2.2.4  .exe file was now created.
