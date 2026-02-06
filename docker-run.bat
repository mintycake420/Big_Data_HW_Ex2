@echo off
REM Script to run Spark tasks in Docker on Windows

echo === Spark IMDB Analysis - Docker Runner ===
echo.

REM Build and start container
echo Building Docker container...
docker-compose build

echo.
echo Starting container...
docker-compose up -d

echo.
echo Container started! Running tasks...
echo.

REM Function equivalent for running tasks
call :run_task 1 "Data Cleaning" "Task1_DataCleaning"
call :run_task 2 "Top Rated by Genre" "Task2_TopRatedByGenre"
call :run_task 3 "Actor Collaboration" "Task3_ActorCollaboration"
call :run_task 4 "High-Rated Hidden Gems" "Task4_HighRatedHiddenGems"
call :run_task 5 "Word Frequency" "Task5_WordFrequency"
call :run_task 6 "Genre Diversity" "Task6_GenreDiversity"
call :run_task 7 "Certification Distribution" "Task7_CertificationDistribution"
call :run_task 8 "TV Shows vs Movies" "Task8_TVvsMovies"

echo === All tasks completed! ===
echo.
echo Output files are in the .\output\ directory
echo.
echo To view results:
echo   dir output\
echo.
echo To enter the container:
echo   docker-compose exec spark-homework bash
echo.
echo To stop the container:
echo   docker-compose down
echo.
pause
exit /b

:run_task
set task_num=%1
set task_name=%~2
set class_name=%~3

echo [%task_num%/8] Running %task_name%...
docker-compose exec -T spark-homework mvn exec:java -Dexec.mainClass="%class_name%" -q
echo ✓ %task_name% completed
echo.
exit /b
