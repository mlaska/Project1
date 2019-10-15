#Two Functions defined below. 1-Calls Census Bureau API and saves csv file for poverty rates of Minneapolis
#                              zipcodes. Also adds additional column defining proverty rate type (Low (rate), 
#                              High(rate) or Downtown)
#
#                             2-Reads CSV files from OpenDate Minneapolis and from the census-api function.
#                               Merges appropriate data and cleans dataframe. Final csv saved as
#                               "data_complete.csv"

#Function (1)census_api. x_value is Poverty Type threshold (%) (default, 10.5--MN average Poverty Rate in 2017)

def census_api(x_value):
    
    import pandas as pd    
    import requests
    from census import Census

    # Census API Key
    from config import api_key
    c = Census(api_key, year=2017)
    
    import warnings
    warnings.filterwarnings('ignore')
    
    # Run Census Search to retrieve data on all zip codes (2013 ACS5 Census)
    # See: https://github.com/CommerceDataService/census-wrapper for library documentation
    # See: https://gist.github.com/afhaque/60558290d6efd892351c4b64e5c01e9b for labels
    census_data = c.acs5.get(("NAME", 
                              "B01003_001E", 
                              "B17001_002E"), {'for': 'zip code tabulation area:*'})

    # Convert to DataFrame
    census_df = pd.DataFrame(census_data)
   

    # Column Reordering
    census_df = census_df.rename(columns={"B01003_001E": "Population",
                                          "B17001_002E": "Poverty Count",
                                          "NAME": "Name", "zip code tabulation area": "Zipcode"})
    
     # Add in Poverty Rate (Poverty Count / Population)
    census_df["Poverty Rate"] = 100 * \
        census_df["Poverty Count"].astype(
            int) / census_df["Population"].astype(int)

    # DataFrame with Poverty Rate
    census_df = census_df[["Zipcode", "Population", "Poverty Count", "Poverty Rate"]]
                           
    census_df["Zipcode"] = census_df["Zipcode"].astype(str)
    
    zip_list = ['55418', '55411', '55405', '55401', '55408', '55414', '55403',
           '55402', '55415', '55404', '55412', '55454', '55406', '55413',
           '55409', '55416', '55419', '55407', '55455', '55422', '55417',
           '55410', '55430', '55487', '55114', '55450', '55421', '55423',
           '55429']   #Hard coded....can get from ID_to_Zipcode program
    
    zipcode_df = pd.DataFrame({"Zipcode": zip_list})

    #Left merge Census dataframe with zipcode list to produce Census dataframe for just metro area 
    
    census_mpls_df = pd.merge(zipcode_df, census_df, on = 'Zipcode', how="left")
    
    #List of downtown zipcodes
    downtown = ['55401', '55402', '55403', '55404', '55405', '55411', '55415', '55454', '55487']

    #x_value is poverty type criteria (currently set at 10.5, MN's overall poverty rate for 2017)

    poverty_type_criteria = float(x_value)     #----function's variable---------

    #Build list to identify if zipcode's poverty type is Low, High, or Downtown
    
    for index, zip in census_mpls_df.iterrows():

        if zip["Zipcode"] in downtown:
            census_mpls_df.set_value(index, 'Poverty Type', 'Downtown')

        elif zip["Poverty Rate"] > poverty_type_criteria: 
            census_mpls_df.set_value(index, 'Poverty Type', 'High')

        else:
            census_mpls_df.set_value(index, 'Poverty Type', 'Low')
            
    poverty_df = census_mpls_df[["Zipcode","Poverty Rate", "Poverty Type"]]
    
    #export to csv
    # Note to avoid any issues later, use encoding="utf-8"
    poverty_df.to_csv("poverty_type.csv", encoding="utf-8", index=False)
    
    print(f"API requested from US Census Bureau")  
    print(f"poverty_type.csv file created with a Poverty Type threshold set at {x_value}%.")                    
    print("")
    print(poverty_df.head())
    
    
#---------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------

#Function (2) read_files 

def read_files():
    
    import pandas as pd
    
    #Read CSVs
    centerline_df = pd.read_csv("Centerline_to_Zipcode.csv")

    scooter_df = pd.read_csv("Scooter_Availability.csv")

    poverty_df = pd.read_csv("poverty_type.csv")
    
    #Create dataframe of just zipcodes and ID to add(merge) onto main dataframe
    centerline_zipcode = pd.DataFrame(centerline_df[['GBSID', 'ZIP5_L']])
    
    #Reclassify Zipcode columns as string fields
    scooter_df['ClosestCenterlineID'] = scooter_df['ClosestCenterlineID'].astype(str)
    centerline_zipcode['GBSID'] = centerline_zipcode['GBSID'].astype(str)
    centerline_zipcode['ZIP5_L'] = centerline_zipcode['ZIP5_L'].astype(str)
    poverty_df['Zipcode'] = poverty_df['Zipcode'].astype(str)
    
    #Create new dataframe from scooter_df and its corresponding Zipcode column Merging on 
    #'CenterlineID' (named 'GBSID' in Minneapolis GIS dataset). 
    scooter_zipcode_df = pd.merge(scooter_df, centerline_zipcode, left_on='ClosestCenterlineID',
                                  right_on = 'GBSID', how="left")
    
    scooter_zipcode_df = scooter_zipcode_df[['PollTime', 'CompanyName', 'NumberAvailable', 
                                             'ClosestCenterlineID', 'Neighborhood', 'ZIP5_L']]
                                        
    scooter_zipcode_df = scooter_zipcode_df.rename(columns = {'ZIP5_L':'Zipcode'})
    
    #Merge poverty type onto dataframe
    scooter_zipcode_df = pd.merge(scooter_zipcode_df, poverty_df, on = 'Zipcode',  how="left")
    
    #Save new df to CSV
       # Note to avoid any issues later, use encoding="utf-8"
    scooter_zipcode_df.to_csv("data_complete.csv", encoding="utf-8", index=False)
    
    print("data_complete.csv file created.")
    print("")
    print("External csv files from Open Data Minneapolis, ")
    print("      http://opendata.minneapolismn.gov/datasets/scooter-availability")
    print("      http://opendata.minneapolismn.gov/datasets/mpls-centerline")      