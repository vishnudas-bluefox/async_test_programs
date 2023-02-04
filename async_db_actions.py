
#!/usr/bin/env python3


from aiohttp import ClientSession

import asyncio

from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.expressions import F
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.models import Throughput,KeySchema,KeySpec, KeyType

import random

#module to calculate the function time
import time
#create items
async def create_item(table,pk):
    async with ClientSession() as session:
        client = Client(AIOHTTP(session),Credentials.auto(),"ap-south-1")
        #create collections of tasks for bulk action performance
        tasks=[]
        #creating bulk of tasks
        print("creating bulk tasks...")
        for i in pk:
            data =i
            tasks.append(asyncio.create_task(table.put_item(i)))
        print("all tasks created...")
        return tasks


#scan limit items
async def scan_items(table,pk):
    pass

#delete all items asyncro
async def delete_all(response,table):
    async with ClientSession() as session:
        client = Client(AIOHTTP(session),Credentials.auto(),"ap-south-1")

        data = [i["userid"] async for i in response]
        task=[]
        #task=[asyncio.create_task(table.deleteItem("userid",i) for i in data]
        for i in data:
              task.append(asyncio.create_task(table.delete_item({"userid":i})))
        print("The tasks are created")
        return task
async def example():

    async with ClientSession() as session:
        client = Client(AIOHTTP(session),Credentials.auto(),"ap-south-1")
        table = client.table("test")
        #create table if doesn't exist
        if not await table.exists():
            await table.create(
                Throughput(read=10, write=10),
                KeySchema(hash_key=KeySpec("Key",KeyType.string)),
            )
        #performing the actions together

        option = int(input("Select the option:\n1.Create_bulkdata\n2.Delete_all_data \nEnter the option:"))
        if option ==1:
            tasks =[]
            #create collections of data
            for _ in range(100):
                data = {
                    "userid":str(random.randint(12445,13674)),
                    "value":str(random.randint(10000,20000))}
                tasks.append(data)
            #passing the datas to create functions
            print("datas created")
            datas = await create_item(table,tasks)
            responses = await asyncio.gather(*datas)
            print("Datas created successfully")
        elif option==6:
            pass
                #delete all the data
        elif option==2:

            response = client.scan("test")
            #getting all the data


            tasks = await delete_all(response,table)
            resp = await asyncio.gather(*tasks)
            print("datas deleted successfully")
asyncio.run(example())
