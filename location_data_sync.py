from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import requests
from bson import ObjectId

conn_str = "mongodb://192.168.0.64:27017/skysoft"  # local
client = MongoClient(conn_str)



saptelData = client.skysoft
country_table = saptelData.countries
state_table = saptelData.states
district_table = saptelData.districts
tehsil_table = saptelData.tehsils
pincode_table = saptelData.pincodes
invalid_pinode = saptelData.duplicatepincodes


def sync_country_data():
    df = pd.read_excel("country.xlsx")
    new_df = df.to_dict("records")
    print(new_df)
    for i in range(0, len(new_df)):
        response = requests.post("https://v60h5qlq-3009.inc1.devtunnels.ms/v1/country/add",json={"countryName": new_df[i]["COUNTRY NAME"], "companyId":str(ObjectId("66e13cdf98a1743c6e977e26"))})
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json() if response.status_code == 200 else response.text}")


def sync_state_data():
    df = pd.read_excel("state.xlsx")
    new_df = df.to_dict("records")
    print(new_df)
    for i in range(0, len(new_df)):
        response =requests.post("https://v60h5qlq-3009.inc1.devtunnels.ms/v1/state/add",json={"stateName": new_df[i]["State Name"], "countryId":str(ObjectId("66e187ade22273068840503b")),"companyId":str(ObjectId("66e13cdf98a1743c6e977e26"))})
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json() if response.status_code == 200 else response.text}")



def sync_district_data():
    df = pd.read_excel("district.xlsx")
    new_df = df.to_dict("records")
    print(new_df)
    for i in range(460, len(new_df)):
        country_data = country_table.find_one({"countryName":new_df[i]["COUNTRY NAME"].lower()})
        state_data = state_table.find_one({"stateName":new_df[i]["STATE NAME"].lower()})
        print(country_data)
        response =requests.post("https://v60h5qlq-3009.inc1.devtunnels.ms/v1/district/add",json={"districtName": new_df[i]["DISTRICT NAME"], "countryId":str(ObjectId(country_data["_id"])),"stateId":str(ObjectId(state_data["_id"])),"companyId":str(ObjectId("66e13cdf98a1743c6e977e26"))})
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json() if response.status_code == 200 else response.text}")



def sync_tehsil_data():
    df = pd.read_excel("tehsil.xlsx")
    new_df = df.to_dict("records")
    for i in range(2737, 3000):
        country_data = country_table.find_one({"countryName":new_df[i]["COUNTRY NAME"].lower()})
        state_data = state_table.find_one({"stateName":new_df[i]["STATE NAME"].lower()})
        print(new_df[i]["DISTRICT NAME"],"ooooooooooooooooooo")
        district_data = district_table.find_one({"districtName":new_df[i]["DISTRICT NAME"].lower()})
        print(district_data,"district_data")
        response =requests.post("https://v60h5qlq-3009.inc1.devtunnels.ms/v1/tehsil/add",json={"tehsilName": new_df[i]["TAHSIL NAME"], "countryId":str(ObjectId(country_data["_id"])),"stateId":str(ObjectId(state_data["_id"])),"districtId":str(ObjectId(district_data["_id"])), "companyId":str(ObjectId("66e13cdf98a1743c6e977e26")) })
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json() if response.status_code == 200 else response.text}")


def sync_pincode_data():
    df = pd.read_excel("pincode.xlsx")
    new_df = df.to_dict("records")
    for i in range(21317, 24000): #0 to 3660 pending
        country_data = country_table.find_one({"countryName":new_df[i]["COUNTRY"].lower()})
        state_data = state_table.find_one({"stateName":new_df[i]["STATE"].lower()})
        district_data = district_table.find_one({"districtName":new_df[i]["DISTRICT"].lower()})
        tehsil_data = tehsil_table.find_one({"tehsilName":new_df[i]["TEHSIL"].lower()})
        
        print(country_data)
        response =requests.post("https://v60h5qlq-3009.inc1.devtunnels.ms/v1/pincode/add",json={"pincode": str(new_df[i]["PINCODE"]), "countryId":str(ObjectId(country_data["_id"])),"stateId":str(ObjectId(state_data["_id"])),"districtId":str(ObjectId(district_data["_id"])),"tehsilId":str(ObjectId(tehsil_data["_id"])), "companyId":str(ObjectId("66e13cdf98a1743c6e977e26")) })
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json() if response.status_code == 200 else response.text}")
        new_res = response.json()
        if new_res["status"]==True:
            print("inn")
        else:
            invalid_pinode.insert_one({"pincode":str(new_df[i]["PINCODE"]),"tehsil":new_df[i]["TEHSIL"].lower()})


# sync_country_data()
# sync_state_data()
# sync_district_data()
# sync_tehsil_data()
sync_pincode_data()
