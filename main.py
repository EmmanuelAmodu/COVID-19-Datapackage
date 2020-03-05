import csv
import json
from datetime import datetime

import requests
import pandas as pd
import numpy as np

from StringIO import StringIO

urls = {
    'confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv',
    'recovered':  'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv',
    'deaths':  'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv',
}


def main(data):
    keys = data.keys()
    countriesData = {}
    for key in keys:
        url = data[key]
        response = requests.get(url, allow_redirects=True)
        # open(fileName, 'wb').write(response.content)

        csv = pd.read_csv(StringIO(response.content))
        countries = csv['Country/Region'].unique()
        print(type(csv))
        for country in countries:
            if country in countriesData:
                pass
            else:
                countriesData[country] = {}

            coutryData = csv[csv['Country/Region'] == country].drop(
                columns=['Province/State', 'Country/Region', 'Lat', 'Long'])
            coutryDataReduced = coutryData.apply(np.sum, axis=0)
            countriesData[country][key] = coutryDataReduced.to_dict()

    sortObject(countriesData)


def sortObject(countriesData):
    countries = []
    for country in countriesData:
        info = {}
        for about in countriesData[country]:
            info['date'] = []
            for date in countriesData[country][about]:
                info['date'].append(date)
                if about in info:
                    pass
                else:
                    info[about] = []
                info[about].append(countriesData[country][about][date])

        countryDate = pd.DataFrame.from_dict(info)
        cleanedCountyName = country.lower().replace(" ", "_")
        countries.append(cleanedCountyName)
        countryDate.to_csv(cleanedCountyName + '.csv', index=False)

    updateDataPackage(countries)


def updateDataPackage(countries):
    resources = []
    for country in countries:
        resources.append({
            "name": country,
            "path": country + ".csv",
            "schema": {
                "fields": [
                    {
                        "name": "confirmed",
                        "type": "number"
                    },
                    {
                        "name": "date",
                        "type": "string"
                    },
                    {
                        "name": "deaths",
                        "type": "number"
                    },
                    {
                        "name": "recovered",
                        "type": "number"
                    }
                ]
            }
        })

        # open data package file
    with open('datapackage.json', 'r+') as datapackage:
        # convert to json
        data = json.load(datapackage)
        # update last_updated field to current time
        data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["resources"] = resources
        # data["resources"] =
        # save write json to file and close
        datapackage.seek(0)
        json.dump(data, datapackage, indent=4)
        datapackage.truncate()


main(urls)
