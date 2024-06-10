import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt



def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="1234",
        database="POBLACION"
    )

def query_db(query):
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    connection.close()
    return df


df = pd.read_csv('/home/gabriel66/Desktop/idjnfie/health_nutrition_population_statistics.csv')

print(df.columns)


for i in range(1960, 2023, 1):
    df = df.rename(columns={f'{i}':f'year_{i}'})

df = df.rename(columns={'Country Name':'Country_Name'})
df = df.rename(columns={'Country Code':'Country_Code'})
df = df.rename(columns={'Indicator Name':'Indicator_Name'})


query = '''
CREATE TABLE Poblacion_global(
    Country_Name VARCHAR(65),
    Country_Code varchar(10),
    Indicator_Name varchar(200),
    year_1960 FLOAT,
    year_1961 FLOAT,
    year_1962 FLOAT,
    year_1963 FLOAT,
    year_1964 FLOAT,
    year_1965 FLOAT,
    year_1966 FLOAT,
    year_1967 FLOAT,
    year_1968 FLOAT,
    year_1969 FLOAT,
    year_1970 FLOAT,
    year_1971 FLOAT,
    year_1972 FLOAT,
    year_1973 FLOAT,
    year_1974 FLOAT,
    year_1975 FLOAT,
    year_1976 FLOAT,
    year_1977 FLOAT,
    year_1978 FLOAT,
    year_1979 FLOAT,
    year_1980 FLOAT,
    year_1981 FLOAT,
    year_1982 FLOAT,
    year_1983 FLOAT,
    year_1984 FLOAT,
    year_1985 FLOAT,
    year_1986 FLOAT,
    year_1987 FLOAT,
    year_1988 FLOAT,
    year_1989 FLOAT,
    year_1990 FLOAT,
    year_1991 FLOAT,
    year_1992 FLOAT,
    year_1993 FLOAT,
    year_1994 FLOAT,
    year_1995 FLOAT,
    year_1996 FLOAT,
    year_1997 FLOAT,
    year_1998 FLOAT,
    year_1999 FLOAT,
    year_2000 FLOAT,
    year_2001 FLOAT,
    year_2002 FLOAT,
    year_2003 FLOAT,
    year_2004 FLOAT,
    year_2005 FLOAT,
    year_2006 FLOAT,
    year_2007 FLOAT,
    year_2008 FLOAT,
    year_2009 FLOAT,
    year_2010 FLOAT,
    year_2011 FLOAT,
    year_2012 FLOAT,
    year_2013 FLOAT,
    year_2014 FLOAT,
    year_2015 FLOAT,
    year_2016 FLOAT,
    year_2017 FLOAT,
    year_2018 FLOAT,
    year_2019 FLOAT,
    year_2020 FLOAT,
    year_2021 FLOAT,
    year_2022 FLOAT,
	PRIMARY KEY(Country_Name, Indicator_Name));
'''

query_db(query)


for i in df.index: 
    x='('
    for j in df.columns:
        x = x + f'{df[i,j]},'
    query_insertar = f'''
    INSERT INTO Poblacion_global (Country_Name,
    Country_Code,
    Indicator_Name,year_1960,
    year_1961,
    year_1962,
    year_1963,
    year_1964,
    year_1965,
    year_1966,
    year_1967,
    year_1968,
    year_1969,
    year_1970,
    year_1971,
    year_1972,
    year_1973,
    year_1974,
    year_1975,
    year_1976,
    year_1977,
    year_1978,
    year_1979,
    year_1980,
    year_1981,
    year_1982,
    year_1983,
    year_1984,
    year_1985,
    year_1986,
    year_1987,
    year_1988,
    year_1989,
    year_1990,
    year_1991,
    year_1992,
    year_1993,
    year_1994,
    year_1995,
    year_1996,
    year_1997,
    year_1998,
    year_1999,
    year_2000,
    year_2001,
    year_2002,
    year_2003,
    year_2004,
    year_2005,
    year_2006,
    year_2007,
    year_2008,
    year_2009,
    year_2010,
    year_2011,
    year_2012,
    year_2013,
    year_2014,
    year_2015,
    year_2016,
    year_2017,
    year_2018,
    year_2019,
    year_2020,
    year_2021,
    year_2022)
    VALUES
    {x[:-1]})
    '''
