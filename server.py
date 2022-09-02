import sys
import asyncio
import aiohttp
import time
import json
import re

API_KEY = "AIzaSyBIeZ5xKL861K1K5Ovins2BWODBfKPBpE4"
ports = {"Juzang": 10168, "Bernard": 10169, "Jaquez": 10170, "Campbell": 10171, "Clark": 10172}
locations = {}
# key is hostname
# value is a list with four elements:
# first element is name of original server that recieved IAMAT
# second element is difference between time client send message and when server received it
# third element is a list with two strings that represent latitude and longitude
# fourth element is the time that the client send the location

def find_fields(message):
    message = re.sub(r'\s+',' ', message).strip()
    message_list = message.split(' ')
    return message_list

def get_location(location_field):
    lat_and_long = ["",""]
    positive = False;
    if location_field[0] != '+' and location_field[0] != '-':
        return -1
    elif location_field[0] == '+':
        positive = True
    for i in range(1, len(location_field)):
        if location_field[i] == '+':
            if positive:
                lat_and_long[0] = location_field[1:i]
            else:
                lat_and_long[0] = location_field[0:i]
            lat_and_long[1] = location_field[i+1:]
            break
        elif location_field[i] == '-':
            if positive:
                lat_and_long[0] = location_field[1:i]
            else:
                lat_and_long[0] = location_field[0:i]
            lat_and_long[1] = location_field[i:]
            break
    return lat_and_long

async def send(writer, reply, log):
    writer.write(reply.encode())
    await writer.drain()
    writer.close()
    log.write(reply)


async def flood(reply, servers, log):
    if "Clark" not in servers:
        if sys.argv[1] == "Jaquez" or sys.argv[1] == "Juzang":
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', ports["Clark"])
                servers.append("Clark")
                log.write("Opened connection to Clark\n")
                await send(writer, "AT " + ','.join(servers) + reply, log)
                log.write("Closed connection to Clark\n")
            except ConnectionRefusedError:
                pass
    if "Jaquez" not in servers:
        if sys.argv[1] == "Clark" or sys.argv[1] == "Bernard":
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', ports["Jaquez"])
                servers.append("Jaquez")
                log.write("Opened connection to Jaquez\n")
                await send(writer, "AT " + ','.join(servers) + reply, log)
                log.write("Closed connection to Jaquez\n")
            except ConnectionRefusedError:
                pass
    if "Juzang" not in servers:
        if sys.argv[1] == "Clark" or sys.argv[1] == "Campbell" or sys.argv[1] == "Bernard":
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', ports["Juzang"])
                servers.append("Juzang")
                log.write("Opened connection to Juzang\n")
                await send(writer, "AT " + ','.join(servers) + reply, log)
                log.write("Closed connection to Juzang\n")
            except ConnectionRefusedError:
                pass
    if "Bernard" not in servers:
        if sys.argv[1] == "Jaquez" or sys.argv[1] == "Juzang" or sys.argv[1] == "Campbell":
            try:
                reader,writer = await asyncio.open_connection('127.0.0.1', ports["Bernard"])
                servers.append("Bernard")
                log.write("Opened connection to Bernard\n")
                await send(writer, "AT " + ','.join(servers) + reply, log)
                log.write("Closed connection to Bernard\n")
            except ConnectionRefusedError:
                pass
    if "Campbell" not in servers:
        if sys.argv[1] == "Bernard" or sys.argv[1] == "Juzang":
            try:
                reader,writer = await asyncio.open_connection('127.0.0.1', ports["Campbell"])
                servers.append("Campbell")
                log.write("Opened connection to Campbell\n")
                await send(writer, "AT " + ','.join(servers) + reply, log)
                log.write("Closed connection to Campbell\n")
            except ConnectionRefusedError:
                pass
            
async def get_places(client, radius):
    radius = float(radius)*1000 # convert radius to meters
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={0},{1}&radius={2}&key={3}".format(locations[client][2][0], locations[client][2][1], radius, API_KEY)
    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                ssl = False,
            ),
    ) as session:
        async with session.get(url) as resp:
            response = await resp.json()
            return response
            
async def main():
    if len(sys.argv) != 2:
        sys.exit("Invalid number of arguments")
    if sys.argv[1] not in ports:
        sys.exit("Invalid server name")
    server = await asyncio.start_server(handle_connection, host='127.0.0.1', port=ports[sys.argv[1]])
    await server.serve_forever()

def bound_results(up_bound, json_object):
    if len(json_object['results']) > int(up_bound):
        json_object['results'] = json_object['results'][0:int(up_bound)]
    json_string = json.dumps(json_object, indent=4)
    return re.sub(r'\n+','\n',json_string).strip()
    
async def handle_connection(reader, writer):
    data = await reader.read()
    log = open(sys.argv[1] + ".txt", 'a')
    message = data.decode()
    reply = "? " + message
    message_list = find_fields(message)
    if message_list[0] == "IAMAT" and len(message_list) == 4:
        location = get_location(message_list[2])
        if location == -1:
            pass
        else:
            try:
                float(location[0]) # check that location field is valid
                float(location[1])
                curr_time = time.time()-float(message_list[3])
                if curr_time >= 0:
                    s_time = "+" + str(curr_time)
                else:
                    s_time = str(curr_time)
                reply = "AT {0} {1} {2} {3} {4}\n".format(sys.argv[1], s_time, message_list[1], message_list[2], message_list[3])
                locations[message_list[1]] = [sys.argv[1], s_time, location, message_list[3]]
            except ValueError:
                pass
    elif message_list[0] == "AT" and len(message_list) == 7:
        servers = message_list[1].split(",")
        log.write("Connected to " + servers[0] + "\n")
        log.write(message)
        log.write("Connection to " + servers[0] + " dropped\n")
        location = get_location(message_list[5])
        flood_servers = True
        if message_list[4] in locations and locations[message_list[4]][3] >= message_list[6]:
            flood_servers = False # server recieved this message already so no need to flood again
        else:
            locations[message_list[4]] = [message_list[2], message_list[3], location, message_list[6]]
        if len(servers) != 5 and flood_servers:
            servers.remove(sys.argv[1])
            servers.insert(0, sys.argv[1])
            reply = ' ' + ' '.join(message_list[2:]) + '\n'
            await flood(reply, servers, log)
        log.close()
        return
    elif message_list[0] == "WHATSAT" and len(message_list) == 4:
        try:
            if float(message_list[2]) > 50 or float(message_list[2]) < 0:
                pass
            elif int(message_list[3]) > 20 or int(message_list[3]) < 0:
                pass
            elif message_list[1] not in locations: # server does not have location of client
                reply = "AT {0} +0 {1} unknwon 0 \n{{}}\n\n".format(sys.argv[1], message_list[1])
            else:
                json_object = await get_places(message_list[1], message_list[2])
                json_output = bound_results(message_list[3], json_object)
                response_list = locations[message_list[1]]
                if float(response_list[2][0]) >= 0:
                    lat = "+" + response_list[2][0]
                else:
                    lat = response_list[2][0]
                if float(response_list[2][1]) >= 0:
                    lang = "+" + response_list[2][1]
                else:
                    lang = response_list[2][1]
                reply = "AT {0} {1} {2} {3}{4} {5}\n{6}\n\n".format(response_list[0], response_list[1], message_list[1], lat, lang, response_list[3], json_output)              
        except ValueError:
            pass
    log.write(message)
    await send(writer, reply, log)
    if reply != "? " + message and message_list[0] == "IAMAT":
        # AT messages to other servers are formmated like:
        # AT list_of_servers original_server server_time_stamp client_name location client_time_stamp
        await flood(reply[2:], [sys.argv[1]], log)
    log.close()


if __name__ == '__main__':
    asyncio.run(main())
