def get_emr_id():
    with open("data/emr_id.txt", "r") as emr_id_file:
        emr_id = emr_id_file.read()
    return emr_id


def set_emr_id(emr_id):
    with open("data/emr_id.txt", "w+") as emr_id_file:
        emr_id_file.write(emr_id)


def start_emr():
    print("Starting EMR")
    set_emr_id("424242")


def stop_emr():
    if get_emr_id():
        print(f"Stopping EMR {get_emr_id()}")
    else:
        print("Cant stop")
    set_emr_id("")


def get_emr_status():
    return "Running"
