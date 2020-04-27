# DLG Data Engineering - Project

### Prerequisites
The data is provided in the zip file "Data Engineering Test.zip" within this there are 2 csv files, copy both the csv files into folder
The csv files are downloaded into /tmp/DLG/DataEngineering/Weather/Input/

### Assumptions
Below assumptions are made for this piece of work
1. It is not required to create any table. Everything is done by creating a dataframe
2. The output is not loaded into a table but will be shown by running the main python script
3. Since this is small piece of work, all the codes are in one module, this can be split into different python scripts
4. The input and output file directory could have been placed within config directory. But this is not created
5. Window function is used without having to partition, since the volume is less there is no performance issue


### Dependencies and data
1. The csv files should be available before running the job
2. There are valid Regions and there are no NULL regions
3. Screen temperature is the temperature that needs to be considered to find the hottest day
  

#### Running tests to ensure everything is working correctly
command to run the unit test case is - pytest tests/test_PythonMain.py


#### Solution overview 
The solution is provided in `./solution/PythonMain.py`

As mentioned above in assumption section, the input and outpur file directory is hardcoded in here but this could have been defined in a config folder

The solution gets the csv files along with the directory structure and copies into a list

Then the solution checks if there are atleast one file to process if yes the solution will continue else a message saying no files to process will be displayed

If csv file exists then a new method - load_parquet, will be called, this method will retrive only columns - region, observationdate and screen temp 
by aggregating on ScreenTemperature

The window function is used to retrieve the maximum temperature

