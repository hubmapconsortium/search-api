from datetime import datetime
from collections import OrderedDict
from operator import getitem

def parse_log(log_file_path):
    print(f"Target file: {log_file_path}")

    with open(log_file_path) as f:
        f = f.readlines()

    entities = {}

    for line in f:
        #print(line)
        parts = line.split(": ")
        time = parts[0][1:20]
        uuid = parts[2].rstrip("\n")

        if uuid not in entities:
            entities[uuid] = {}
            entities[uuid]['start'] = time
        else:
            entities[uuid]['end'] = time
            entities[uuid]['duration'] = int((datetime.fromisoformat(entities[uuid]['end']) - datetime.fromisoformat(entities[uuid]['start'])).total_seconds())
 

    # Only care about the completed
    filtered_entities = {k:v for k,v in entities.items() if 'duration' in v}
    #print(filtered_entities)

    sorted_filtered_entities = OrderedDict(sorted(filtered_entities.items(), key = lambda x: getitem(x[1], 'duration')))
    #print(sorted_filtered_donors)

    for uuid in sorted_filtered_entities:
        print(f"Entity: {uuid} Total index time: {sorted_filtered_entities[uuid]['duration']} seconds")


if __name__ == "__main__":
    try:
        log_file_path = sys.argv[1]
        parse_log(log_file_path)
    except IndexError as e:
        msg = "Missing log file path argument"
        logger.exception(msg)
        sys.exit(msg)

    