import pytest
from solution.PythonMain import get_max_temp

@pytest.mark.usefixtures("spark")
def test_simple_get_max_temp(spark):
    df = spark.createDataFrame([{"region": "Northern Ireland", "Observation_Date": "2016-03-07", "Screen_Temperature": 7.40},
                           {"region": "Grampian", "Observation_Date": "2016-03-12", "Screen_Temperature": 11.70}])

    expected = spark.createDataFrame([{"region":"Grampian","Observation_Date":"2016-03-12","Screen_Temperature":11.70}]).collect()

    actual = get_max_temp(df).collect()

    print(expected)
    print(actual)

    assert actual == expected

def test_multiple_region_get_max_temp(spark):
    df = spark.createDataFrame([{"region": "Northern Ireland", "Observation_Date": "2016-03-07", "Screen_Temperature": 7.40},
                           {"region": "Grampian", "Observation_Date": "2016-03-12", "Screen_Temperature": 11.70},
                           {"region": "Strathclyde", "Observation_Date": "2016-03-12", "Screen_Temperature": 11.70}])

    expected = spark.createDataFrame([{"region":"Grampian","Observation_Date":"2016-03-12","Screen_Temperature":11.70},{"region": "Strathclyde", "Observation_Date": "2016-03-12", "Screen_Temperature": 11.70}]).collect()

    actual = get_max_temp(df).collect()

    print(expected)
    print(actual)

    assert actual == expected
