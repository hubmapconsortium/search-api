from datetime import datetime
from collections import OrderedDict
from operator import getitem

# New sections registered for Sample/organ HBM742.FCHF.843 (8b34faba613d743dab7392bd22721ddb)
# whose Donor uuid is 3a0960c7cc8864dcc003165cef9ca040

file = r"./donors.log"

def parse_log():
    with open(file) as f:
        f = f.readlines()

    donors = {}

    for line in f:
        #print(line)
        parts = line.split(": ")
        time = parts[0][1:20]
        uuid = parts[2].rstrip("\n")

        if uuid not in donors:
            donors[uuid] = {}
            donors[uuid]['start'] = time
        else:
            donors[uuid]['end'] = time
            donors[uuid]['duration'] = int((datetime.fromisoformat(donors[uuid]['end']) - datetime.fromisoformat(donors[uuid]['start'])).total_seconds())
 

    # Only care about the completed
    filtered_donors = {k:v for k,v in donors.items() if 'duration' in v}
    #print(filtered_donors)

    sorted_filtered_donors = OrderedDict(sorted(filtered_donors.items(), key = lambda x: getitem(x[1], 'duration')))
    #print(sorted_filtered_donors)

    for uuid in sorted_filtered_donors:
        print(f"Donor: {uuid} Total index time: {sorted_filtered_donors[uuid]['duration']} seconds")


if __name__ == "__main__":
    parse_log()