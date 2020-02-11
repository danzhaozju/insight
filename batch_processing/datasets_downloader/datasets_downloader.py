### File to download bike dataset
bike_txt = 'bike_index.txt'

bike_result = []
with open(bike_txt, "r") as f:
    for line in f:
        pos1 = 1
        pos2 = line.find('.zip') + 4
        substring = line[pos1:pos2]
        new_line = "wget -P /home/ubuntu/datasets/ " + "https://tripdata.s3.amazonaws.com/" + substring
        bike_result.append(new_line)

with open('bike_download_links.sh','w') as f:
    f.writelines('%s\n' % line for line in bike_result)


### File to download taxi dataset
taxi_txt = 'taxi_index.txt'

taxi_result = []
with open(taxi_txt, "r") as f:
    for line in f:
        pos_green = line.find('green')
        pos_yellow = line.find('yellow')
        pos_last = line.find('.csv') + 4
        if pos_green >= 0:
            substring = line[pos_green:pos_last]
        elif pos_yellow >= 0:
            substring = line[pos_yellow:pos_last]
        else:
            continue
        new_line = "wget -P /home/ubuntu/datasets/ " + "https://nyc-tlc.s3.amazonaws.com/trip+data/" + substring
        taxi_result.append(new_line)

with open('taxi_download_links.sh','w') as f:
    f.writelines('%s\n' % line for line in taxi_result)

