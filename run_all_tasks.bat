@echo off
REM Batch script to run all Spark tasks sequentially

echo ========================================
echo Running All IMDB Analysis Tasks
echo ========================================
echo.

echo [1/8] Running Task 1: Data Cleaning...
call mvn exec:java -Dexec.mainClass="Task1_DataCleaning"
echo.

echo [2/8] Running Task 2: Top Rated by Genre...
call mvn exec:java -Dexec.mainClass="Task2_TopRatedByGenre"
echo.

echo [3/8] Running Task 3: Actor Collaboration...
call mvn exec:java -Dexec.mainClass="Task3_ActorCollaboration"
echo.

echo [4/8] Running Task 4: High-Rated Hidden Gems...
call mvn exec:java -Dexec.mainClass="Task4_HighRatedHiddenGems"
echo.

echo [5/8] Running Task 5: Word Frequency...
call mvn exec:java -Dexec.mainClass="Task5_WordFrequency"
echo.

echo [6/8] Running Task 6: Genre Diversity...
call mvn exec:java -Dexec.mainClass="Task6_GenreDiversity"
echo.

echo [7/8] Running Task 7: Certification Distribution...
call mvn exec:java -Dexec.mainClass="Task7_CertificationDistribution"
echo.

echo [8/8] Running Task 8: TV Shows vs Movies...
call mvn exec:java -Dexec.mainClass="Task8_TVvsMovies"
echo.

echo ========================================
echo All tasks completed!
echo Check the output/ directory for results
echo ========================================
pause
