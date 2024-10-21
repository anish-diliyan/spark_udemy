# Git setup
git config --local --get user.name
git config --local --get user.email 

git config --local user.name "Anish Kumar"
git config --local user.email "anish.diliyan@gmail.com"

# "origin" is the remote name, and the URL is the HTTPS URL of the repository on GitHub.
git remote get-url origin

# SetUp pgsql using Zip file (for window user only)
1. Download Zip file for window 64 bit and extract.
2. Set bin folder in env variable path.
3. Create a bat file to start pgsql from cmd using command "start_pg"
```shell
@echo off
echo Starting PostgreSQL...
"D:\installed\pgsql\bin\pg_ctl.exe" -D "D:\pgsql_data" start
if %ERRORLEVEL% EQU 0 (
    echo PostgreSQL started successfully.
) else (
    echo Failed to start PostgreSQL. Error code: %ERRORLEVEL%
)
pause
```
4. Create a bat file "stop_pg.bat" to stop pgsql from cmd using "stop_pg"
```shell
@echo off
echo Starting PostgreSQL...
"D:\installed\pgsql\bin\pg_ctl.exe" -D "D:\pgsql_data" stop
if %ERRORLEVEL% EQU 0 (
    echo PostgreSQL shutdown successfully.
) else (
    echo Failed to shutdown PostgreSQL. Error code: %ERRORLEVEL%
)
pause
```
5. initiate a pg database server using following command (this is one time activity), but first create 
pgsql_data this folder
```shell
initdb.exe -D D:\pgsql_data -U postgres -W -E UTF8 -A scram-sha-256
```

# SetUp database
1. CREATE DATABASE employees;
2. CREATE USER udemy with password 'pgsql';
3. GRANT ALL PRIVILEGES ON employees.* TO 'udemy'@'%';
4. FLUSH PRIVILEGES;
5. psql -U udemy -d employees -f employees_db.sql

# Query database employees
1. run command start_pg from cmd
2. in another terminal psql -U udemy -d employees
3. execute the queries.









