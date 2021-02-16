# ftf-re-api
## API Setup Steps
1. Clone the repo
2. Make sure you have python3 installed and this is the version you are using
3. In the terminal:
  - After cloning the repo, navigate to its directory (ftf-re-api) and create a virtual environment (make sure that the environment is called 'venv')
    - Run: `python3 -m venv venv`
  - Activate the environment by running Mac: `source venv/bin/activate`, Windows: `./venv/scripts/activate`
  - Install dependencies with pip
    - `pip install -r requirements.txt`
4. Setup MySql to run in local machine and setup credentials in .env file (steps below adapted from: https://codeburst.io/installing-and-configuring-mysql-with-django-a7b54b0f27ce)
  - Mac users:
    - MySQL install with homebrew: `brew install mysql`
    - Start MySQL with homebrew: `brew services start mysql`
    - Open MySQL Workbench
  - Windows users:
    - Download MySQLInstaller: https://dev.mysql.com/downloads/installer/
    - If you installed MySQLWorkbench before MySQLInstaller, remove MySQLWorkbench with MySQLInstaller
    - Click "Add.." in MySQLInstaller
    - Select the developer kit, leave defaults as is and install
    - Once everything is installed, click "reconfigure" on the MySQLServer install
    - Give a name to the MySQL Server and start it
    - Open MySQL Workbench
  - In MySQL Workbench, under connections, right click and rescan servers
  - Double click on the local instance connection just created
  - Create a new Schema
    - Click the Schema tab
    - Right-click in the Schema list and select Create Schema
    - Give it a name and follow the prompts to create it
  - Setup a database user
    - Select the Administration tab
    - Select Users and Privileges
    - Click Add Account
    - Give your user a name and password
    - Assign Administrative Roles and Schema Privileges
    - ![alt text](https://miro.medium.com/max/875/1*02UwfaPiNr8mWqSqb3akdg.png)
    
    *If you press on the DBA checkmark it should check all of them, and you're good to go*
5. Create a .env file inside 'ftf-re-api/reporting_engine/'
     - Add these lines to the file:
     ```
       export DB_NAME='[db name you assigned to the schema you created in workbench]'
       export DB_USER='[db user name you created]'
       export DB_PW='[db user password you created]'
       export DB_HOST='127.0.0.1'
       export DB_PORT='3306'
      ```
     *Replace what's in square brackets, including the square brackets - example: '[db name...]' -> 'reports_beta'*
6. Connect Django to DB and start Django Server
  - Navigate to 'ftf-re-api/reporting-engine/'
  - Make sure MySQL server is running
  - Run: `python manage.py migrate`
  - Start django server: `python manage.py runserver`
7. If everything was successful, you should be able to navigate to http://localhost:8000 and see django's default index page
8. To navigate to the admin site visit: 'http://localhost:8000/admin'
  - Josean Martinez has the credentials for the super user, let him know to see how he can get that to you.
  - If not, you could also create your own admin superuser
    - Run: `python manage.py createsuperuser`
    - With the username and password you specify here, you can access the django admin
  - The Django admin is a user interface to see the entries in the database and manage them (modify, delete, add, etc...)
  
**After these steps are finished, all you have to do when coming back to work on the project is to activate the python environment: `source ./venv/bin/activate` and then you can start the django server**
