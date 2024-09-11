import socket
import json
import os
import shutil
import re
from prettytable import PrettyTable

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ASCENDING

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind(('localhost', 8989))
bufferSize = 1024
used_database = ""

uri = "mongodb+srv://..."

# Create a new client and connect to the server
mongo_client = MongoClient(uri, server_api=ServerApi('1'))


def read_json_file(file_path="Catalog.json"):
    try:
        with open(file_path, 'r') as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}.")
        return None


def write_json_file(data, file_path="Catalog.json"):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    except IOError as e:
        print(f"Error writing to {file_path}: {e}")


def create(params):
    data = read_json_file()
    if params[0].lower() == "database":
        if data == {}:
            data = {"databases": {}}
        data["databases"][params[1]] = ({"name": params[1], "tables": {}})
        try:
            write_json_file(data)
            os.makedirs("databases/" + params[1] + "/tables")
            server_response = "Database created successfully {}".format(
                params[1])
            server_socket.sendto(server_response.encode(), address)
        except FileExistsError as e:
            server_response = "Database {} already exists. Choose a different name".format(params[1])
            server_socket.sendto(server_response.encode(), address)
    elif params[0].lower() == "table":
        if used_database:
            table_name = params[1]
            params = params[2:]
            cmd_text = ''
            for attribute in params:
                cmd_text += attribute + ' '
            cmd_text = cmd_text[1:-3]  # remove first and final paranthesis
            cmd_text = cmd_text.split(', ')

            structure = []
            primaryKey = []
            indexAttributes = []
            indexFiles = []
            attributes_names = []
            foreign_keys = []
            foundIndexes = 0
            uniqueKeys = []
            for line in cmd_text:
                line = line.split(' ')
                if line[0] == "":
                    line = line[1:]  # remove empty elements
                # if line starts with primary key
                if line[0].lower().startswith("primary"):
                    refered_attributes = line[2][1:-1].split(',')
                    for atr in refered_attributes:
                        primaryKey.append({"attributeName": atr})
                        # indexAttributes.append({"attributeName": atr})
                        # uniqueKeys.append({"attributeName": atr})
                        # foundIndexes = 1
                else:
                    atr_name = line[0]
                    atr_type = line[1]

                    if '(' in atr_type:  # check if there's a declared length
                        length = atr_type.split('(', 1)[1]
                        length = length[:-1]
                        type = atr_type.split('(', 1)[0]
                    else:
                        length = "1"
                        type = atr_type

                    structure.append({"attributeName": atr_name, "type": type, "length": length})

                    if line[-2].lower() == 'primary' and line[-1].lower() == 'key':
                        primaryKey.append({"attributeName": line[0]})
                        # indexAttributes.append({"attributeName": line[0]})
                        # uniqueKeys.append({"attributeName": line[0]})
                        # foundIndexes = 1

                    if line[-1].lower() == "unique":  # check for any unique key
                        uniqueKeys.append({"attributeName": line[0]})
                        indexAttributes.append({"attributeName": line[0]})
                        foundIndexes = 1

                    # Check if the word "reference" exists in any of the strings in the array (case-insensitive)
                    found = any("references" in string.lower() for string in line)

                    if found:
                        refered_table = line[3]
                        refered_attribute = line[4][1:-1]
                        tables = data["databases"][used_database]["tables"]
                        if refered_table not in tables:
                            msg = "FK references invalid table {}".format(refered_table)
                            server_socket.sendto(msg.encode(), address)
                        else:
                            fk_structure = data["databases"][used_database]["tables"][refered_table]["structure"]
                            for fk_attribute in fk_structure:
                                attributes_names.append(fk_attribute["attributeName"])

                            if refered_attribute not in attributes_names:
                                msg = "FK references invalid column {} in referenced table {}".format(refered_attribute,
                                                                                                      refered_table)
                                server_socket.sendto(msg.encode(), address)
                                return
                            else:
                                foreign_keys.append({"foreignKey": atr_name, "refTable": refered_table,
                                                     "refAttribute": refered_attribute})
                                indexAttributes.append({"attributeName": atr_name})
                                foundIndexes = 1

            uniq_pks = []
            # Assuming there's at least one index declared (foundIndexes == 1)
            if foundIndexes == 1:
                indexAttributesList = []

                # # Collect all primary key attributes
                # for pk_attr in primaryKey:
                #     attribute_value = pk_attr.get("attributeName")
                #     if attribute_value:
                #         indexAttributesList.append(pk_attr)
                #         uniq_pks.append(attribute_value)

                # Create a single index file with a composite structure
                if indexAttributesList:
                    composite_index_name = table_name + 'Index' + ''.join(
                        attr['attributeName'] for attr in indexAttributesList)
                    composite_key_length = sum(len(attr['attributeName']) for attr in indexAttributesList)

                    indexFiles.append({
                        "indexName": composite_index_name,
                        "keyLength": composite_key_length,
                        "isUnique": True,  # Assuming this is a boolean flag for composite key
                        "indexType": "BTree",
                        "indexAttributes": indexAttributesList
                    })

            uniq = []
            if foundIndexes == 1:  # check is there are declared indexes
                for index_attr in indexAttributes:
                    # Assuming index_attr has one key-value pair and we want the value
                    attribute_value = next(iter(index_attr.values()))
                    if (attribute_value not in uniq) and (attribute_value not in uniq_pks):
                        uniq.append(attribute_value)
                        indexFiles.append({
                            "indexName": table_name + 'Index' + attribute_value,
                            "keyLength": len(attribute_value),  # Assuming you want the length of the attribute value
                            "isUnique": True,  # Assuming this is a boolean flag
                            "indexType": "BTree",
                            "indexAttributes": index_attr
                        })

            table = {"tableName": table_name, "fileName": table_name + ".bin", "rowLength": len(cmd_text),
                     "structure": structure,
                     "primaryKey": primaryKey,
                     "foreignKeys": foreign_keys,
                     "uniqueKeys": uniqueKeys,
                     "indexFiles": indexFiles}
            data["databases"][used_database]["tables"][table_name] = table
            write_json_file(data)
            server_response = "Commands completed successfully."
            server_socket.sendto(server_response.encode(), address)

    elif params[0].lower() == "index":
        if used_database:
            indexName = params[1]
            tableName = params[3]
            params = params[4:]  # remove index name and table name from params
            indexFiles = data["databases"][used_database]["tables"][tableName]["indexFiles"]
            structure = data["databases"][used_database]["tables"][tableName]["structure"]
            foundAttributes = True
            indexAttributes = []
            column_names = []

            cmd_text = ''
            for attribute in params:
                cmd_text += attribute + ' '
            cmd_text = cmd_text[1:-3]
            cmd_text = cmd_text.split(', ')

            for attribute in structure:
                column_names.append(attribute["attributeName"])
            for attribute in cmd_text:
                if attribute[0] == " ":
                    attribute = attribute[1:]  # check for white spaces after comma
                if attribute not in column_names:  # check if given attribute exists
                    server_response = "Column does not exist in table"
                    server_socket.sendto(server_response.encode(), address)
                    foundAttributes = False
                else:
                    indexAttributes.append({"attributeName": attribute})
            if foundAttributes == True:
                indexFiles.append({"indexName": indexName + 'Index', "keyLength": len(indexName), "isUnique": "1",
                                   "indexType": "BTree",
                                   "indexAttributes": indexAttributes})
                data["databases"][used_database]["tables"][tableName]["indexFiles"] = indexFiles
                write_json_file(data)
                server_response = "Commands completed successfully"
                server_socket.sendto(server_response.encode(), address)
        else:
            server_response = "Database is not in use"
            server_socket.sendto(server_response.encode(), address)

    else:
        server_response = "Invalid column name"
        server_socket.sendto(server_response.encode(), address)


def use(params):
    data = read_json_file()
    db = data["databases"].get(params[0], None)
    if db:
        global used_database
        used_database = params[0]
        server_response = "Commands completed successfully"
        server_socket.sendto(server_response.encode(), address)
    else:
        server_response = "Database does not exist. Make sure that the name is entered correctly"
        server_socket.sendto(server_response.encode(), address)


def drop(params):
    data = read_json_file()
    global used_database
    if params[0].lower() == "database":
        db_data = data["databases"].get(params[1], None)
        if db_data:
            db = params[1]
            del data["databases"][db]
            write_json_file(data)
            shutil.rmtree("databases/" + db)
            msg = "Commands completed successfully."
            server_socket.sendto(msg.encode(), address)
            if used_database == db:
                used_database = ""
        else:
            msg = "Cannot drop the database, because it does not exist."
            server_socket.sendto(msg.encode(), address)
    elif params[0].lower() == "table":
        if used_database:
            table = data["databases"][used_database]["tables"].get(params[1], None)
            if table:
                tables = data["databases"][used_database]["tables"]
                fk_constraint = False
                for auxTable in tables:
                    foreignKeys = data["databases"][used_database]["tables"][auxTable]["foreignKeys"]
                    for fk in foreignKeys:
                        if fk["refTable"] == table["tableName"]:
                            fk_constraint = True

                if fk_constraint == True:
                    server_response = "Could not drop object because it is referenced by a FOREIGN KEY constraint."
                    server_socket.sendto(server_response.encode(), address)
                else:
                    table_name = params[1]
                    db = mongo_client[used_database]
                    collection = db[table_name]
                    collection.drop()

                    index_files = data["databases"][used_database]["tables"][table_name]["indexFiles"]
                    for index_file in index_files:
                        file_name = index_file['indexName'] + ".ind"
                        collection = db[file_name]
                        collection.drop()

                    del data["databases"][used_database]["tables"][params[1]]
                    write_json_file(data)

                    server_response = "Commands completed successfully."
                    server_socket.sendto(server_response.encode(), address)
            else:
                server_response = "Cannot drop the table, because it does not exist"
                server_socket.sendto(server_response.encode(), address)
        else:
            server_response = "Cannot drop the database, because it does not exist"
            server_socket.sendto(server_response.encode(), address)


def check_type(attribute, value, length=None):
    try:
        if value == "int":
            int(attribute)
            return True
        elif value == "float":
            float(attribute)
            return True
        elif value in ["varchar", "char"]:
            return isinstance(attribute, str) and len(attribute) <= int(length)
        elif value == "bool":
            return isinstance(attribute, bool)
        else:
            return False
    except (ValueError, TypeError):
        return False


from pymongo import MongoClient


def load_index(foreign_keys, document_id, path, collection):
    # Read the existing index from the file
    index = {}
    try:
        with open(path, "r") as f:
            for line in f:
                if line.startswith("key: "):
                    key = line.strip().split(": ")[1]
                elif line.startswith("value: "):
                    value = line.strip().split(": ")[1]
                    index[key] = value.split('#')
    except FileNotFoundError:
        pass  # It's okay if the file doesn't exist yet

    # Fetch documents and update the index
    for key in foreign_keys:
        if key in index.keys():
            index[key].append(str(document_id))
        else:
            index[key] = [str(document_id)]

    # Ensure there's an index for each foreign key in the collection
    for key in foreign_keys:
        # Create an index if it doesn't exist
        collection.create_index([(key, ASCENDING)])

        # Update the document with the new foreign key value
        collection.update_one(
            {key: {"$exists": True}},
            {"$addToSet": {key: document_id}}
        )

    # Write the index to a file
    with open(path, "w") as f:
        for key, ids in index.items():
            f.write(f"key: {key}\n")
            f.write(f"value: {'#'.join(ids)}\n\n")


def insert(params):
    data = read_json_file()
    global used_database
    if (params[0].lower() != "into") or (len(params) < 3):
        server_socket.sendto("INVALID INSERT COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(params[1], None)
            if table:
                table_name = params[1]
                db = mongo_client[used_database]
                collection = db[table_name]
                key = ''
                composite_keys = data["databases"][used_database]["tables"][table_name]["primaryKey"]
                composite_key_values = []
                value = ""
                params = params[2:]
                params = " ".join(params)
                inserted = False

                # Extracting column names
                column_names = re.search(r"\((.*?)\)", params).group(1).split(',')
                column_names = [name.strip() for name in column_names]

                # Extracting column values
                column_values_match = re.search(r"values\s*\((.*?)\);", params).group(1)
                column_values = [val.strip("'") for val in
                                 re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", column_values_match)]
                if len(column_names) != data["databases"][used_database]["tables"][table_name]["rowLength"]:
                    server_socket.sendto("THE NUMBER OF ATTRIBUTES DIFFER!".encode(), address)
                else:
                    catalog_structure = data["databases"][used_database]["tables"][table_name]["structure"]
                    for i in range(len(column_names)):
                        for j in range(len(column_names)):
                            if (catalog_structure[j]["attributeName"] == column_names[i]):
                                if not check_type(column_values[i], catalog_structure[j]["type"],
                                                  catalog_structure[j]["length"]):
                                    server_socket.sendto("ATTRIBUTE TYPES DO NOT MATCH!".encode(), address)
                                    break
                                else:
                                    if catalog_structure[j]["attributeName"] in [key["attributeName"] for key in
                                                                                 composite_keys]:
                                        composite_key_values.append(column_values[i])
                                    else:
                                        value += column_values[i] + "#"
                    key = '$'.join(composite_key_values)
                    if collection.find_one({"_id": key}):
                        server_socket.sendto(
                            "DATA WITH THAT PRIMARY KEY ALREADY EXISTS IN TABLE {}".format(table_name).encode(),
                            address)
                    else:
                        if len(data["databases"][used_database]["tables"][table_name]["foreignKeys"]) > 0:
                            fk = data["databases"][used_database]["tables"][table_name]["foreignKeys"]
                            fk_invalid = 0

                            if len(fk) > 0:
                                fk_list = []
                                fk_list_names = []
                                for i in range(len(column_names)):
                                    for fk_column in fk:
                                        collection_fk = db[fk_column["refTable"]]
                                        if column_names[i] == fk_column["refAttribute"]:
                                            if not collection_fk.find_one({"_id": column_values[i]}):
                                                server_socket.sendto(
                                                    "FOREIGN KEY {} DOES NOT EXIST IN TABLE {}".format(column_names[i],
                                                                                                       fk_column[
                                                                                                           "refTable"]).encode(),
                                                    address)
                                                return
                                            else:
                                                fk_list.append(column_values[i])
                                                fk_list_names.append(column_names[i])

                                if fk_invalid != 1:
                                    post = {"_id": key, "value": value}
                                    insert_result = collection.update_one({"_id": key}, {"$set": {"value": value}},
                                                                          upsert=True)
                                    inserted = True
                                    document_id = insert_result.upserted_id
                                    for fk in range(len(fk_list)):
                                        collection_index_fk = db[
                                            table_name + "_foreignKey" + fk_list_names[fk] + ".ind"]
                                        # case 1: append if key already exists
                                        doc = collection_index_fk.find_one({"_id": fk_list[fk]})
                                        if doc:
                                            new_value = doc['value'] + '#' + document_id
                                            insert_result_index = collection_index_fk.update_one({"_id": fk_list[fk]},
                                                                                                 {"$set": {
                                                                                                     "value": new_value}},
                                                                                                 upsert=True)
                                        else:
                                            # case 2: create new key and value lines
                                            insert_result_index = collection_index_fk.update_one({"_id": fk_list[fk]},
                                                                                                 {"$set": {
                                                                                                     "value": key}},

                                                                                                 upsert=True)

                        if len(data["databases"][used_database]["tables"][table_name]["uniqueKeys"]) > 0:
                            done = False
                            for uk in data["databases"][used_database]["tables"][table_name]["uniqueKeys"]:
                                uKey = ""
                                if len(uk) > 0 and not done:
                                    for i in range(len(column_names)):
                                        if column_names[i] == uk["attributeName"]:
                                            collection_index_uk = db[
                                                table_name + "_uniqueKey" + uk["attributeName"] + ".ind"]
                                            doc = collection_index_uk.find_one({"_id": column_values[i]})
                                            if doc:
                                                server_socket.sendto("UNIQUE KEY CONSTRAINT VIOLATED IN TABLE".encode(),
                                                                     address)
                                                return;
                                            else:
                                                uKey += column_values[i]
                                    if uKey != "":
                                        collection_index_uk = db[
                                            table_name + "_uniqueKey" + uk["attributeName"] + ".ind"]
                                        insert_result_index = collection_index_uk.update_one({"_id": uKey},
                                                                                             {"$set": {
                                                                                                 "value": key}},
                                                                                             upsert=True)
                                        collection.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
                                        inserted = True
                        if len(data["databases"][used_database]["tables"][table_name]["indexFiles"]) > 0:
                            index = data["databases"][used_database]["tables"][table_name]["indexFiles"]

                            # Create a dictionary mapping column names to values
                            name_to_value_map = dict(zip(column_names, column_values))
                            index_names = []
                            index_array = []
                            index_files = data["databases"][used_database]["tables"][table_name]["indexFiles"]
                            for inx in index_files:
                                if inx["indexName"].startswith("idx"):
                                    if isinstance(inx["indexAttributes"], list):  # Check if indexAttributes is a list
                                        attribute_values = [name_to_value_map.get(attr["attributeName"], "") for attr in
                                                            inx["indexAttributes"]]
                                        combined_value = "$".join(attribute_values)
                                        index_names.append(inx["indexName"])
                                        index_array.append(combined_value)
                                    else:  # indexAttributes is a single dictionary
                                        attribute_name = inx["indexAttributes"]["attributeName"]
                                        attribute_value = name_to_value_map.get(attribute_name, "")
                                        index_names.append(inx["indexName"])
                                        index_array.append(attribute_value)

                            insert_result = collection.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
                            if len(index_array) > 0:
                                inserted = True
                                if insert_result.upserted_id is not None:
                                    document_id = insert_result.upserted_id
                                else:
                                    document_id = key

                                for j in range(len(index_array)):
                                    collection_index_idx = db[index_names[j] + ".ind"]
                                    # case 1: append if key already exists
                                    doc = collection_index_idx.find_one({"_id": index_array[j]})
                                    if doc:
                                        new_value = doc['value'] + '#' + document_id
                                        insert_result_index = collection_index_idx.update_one({"_id": index_array[j]},
                                                                                              {"$set": {
                                                                                                  "value": new_value}},
                                                                                              upsert=True)
                                    else:
                                        # case 2: create new key and value lines
                                        insert_result_index = collection_index_idx.update_one({"_id": index_array[j]},
                                                                                              {"$set": {
                                                                                                  "value": key}},
                                                                                              upsert=True)
                        if not inserted:
                            collection.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
                        server_socket.sendto("DATA INSERTED INTO {}".format(table_name).encode(), address)
            else:
                msg = "TABLE DOES NOT EXIST"
                server_socket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            server_socket.sendto(msg.encode(), address)


def search_fk(refered_attribute, used_database, data, searched_value, db):
    tables = data["databases"][used_database]["tables"]
    for table in tables:
        fks = data["databases"][used_database]["tables"][table]["foreignKeys"]
        for fk_attribute in fks:
            if fk_attribute["foreignKey"] == refered_attribute:
                collection = db["foreignKey" + refered_attribute + ".ind"]
                if collection.find_one({"_id": searched_value}):
                    return True
    return False


def delete(params):
    data = read_json_file()
    global used_database
    if (params[0].lower() != "from") or (len(params) < 2):
        server_socket.sendto("INVALID DELETE COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(params[1], None)
            if table:
                table_name = params[1]
                db = mongo_client[used_database]
                collection = db[table_name]
                keys = ''
                deleted = False

                table_data = [doc['_id'] for doc in collection.find({}, {'_id': 1})]
                if len(params) == 2:
                    collection.delete_many({})
                    server_socket.sendto("ALL DATA REMOVED FROM {}".format(table_name).encode(), address)
                else:
                    if params[2].lower() != "where":
                        server_socket.sendto("INVALID DELETE COMMAND".encode(), address)
                    else:
                        params = params[3:]
                        params = list(filter(lambda x: x.lower() != 'and', params))

                        attributes = data["databases"][used_database]["tables"][table_name][
                            "structure"]  # remove all occurences of 'and'
                        column_names = []
                        column_values = []
                        mongo_index = []
                        for st in params:
                            attPos = -1
                            for pos in range(len(attributes)):
                                if st.split('=')[0] == attributes[pos]["attributeName"]:
                                    attPos = pos
                                    break
                            if attPos == -1:
                                server_socket.sendto("ATTRIBUTE {} DOES NOT EXIST IN TABLE {}".format(st.split('=')[0],
                                                                                                      table_name).encode(),
                                                     address)
                                break
                            else:
                                column_names.append(st.split('=')[0])
                                column_values.append(st.split('=')[1])
                                mongo_index.append(attPos)

                        for td in table_data:
                            found = True
                            doc = collection.find_one({"_id": td})

                            # Check if a matching document was found
                            if doc:
                                td_values = doc['value']
                                td_values = td_values.split("#")
                                td_values = td_values[:-1]
                                td_values.insert(0, doc['_id'])
                                column_index = 0;
                                for i in mongo_index:
                                    if column_values[column_index] != td_values[i]:
                                        found = False
                                    column_index += 1
                                if found:

                                    deleted = False
                                    if search_fk(st.split('=')[0], used_database, data, st.split('=')[1], db) == False:
                                        # Find the documents to delete and get their _ids
                                        query = {"_id": td}
                                        ids_to_delete = [doc['_id'] for doc in collection.find(query, {'_id': 1})]
                                        unique_attrs = data["databases"][used_database]["tables"][table_name][
                                            "uniqueKeys"]  # remove all occurences of 'and'
                                        for uniq_attr in unique_attrs:
                                            collection_index_idx = db[
                                                table_name + "_uniqueKey" + uniq_attr["attributeName"] + ".ind"]
                                            for id_to_delete in ids_to_delete:
                                                all_data = [doc['value'] for doc in
                                                            collection_index_idx.find({}, {'value': 1})]
                                                all_ids = [doc['_id'] for doc in
                                                           collection_index_idx.find({}, {'_id': 1})]
                                                for j in range(len(all_data)):
                                                    elems = all_data[j].split("#")
                                                    if len(elems) <= 1:
                                                        elems = [all_data[j]]

                                                    ok = 1
                                                    new_val = ""
                                                    for elem in elems:
                                                        if elem in ids_to_delete:
                                                            deleted = True
                                                        else:
                                                            new_val = new_val + elem + "#"
                                                    new_val = new_val[:-1]

                                                    if deleted:
                                                        if new_val == "":
                                                            collection_index_idx.delete_one({"_id": all_ids[j]})
                                                        else:
                                                            insert_result = collection_index_idx.update_one(
                                                                {"_id": all_ids[j]},
                                                                {"$set": {"value": new_val}},
                                                                upsert=True)

                                        collection.update_many({st.split('=')[0]: st.split('=')[1]},
                                                               {"$pull": {st.split('=')[0]: st.split('=')[1]}})
                                        deleted = True

                                        deleted = False
                                        fks = data["databases"][used_database]["tables"][table_name]["foreignKeys"]
                                        for fk_attribute in fks:
                                            collection_index_idx = db[
                                                table_name + "_foreignKey" + fk_attribute["foreignKey"] + ".ind"]
                                            documents = [doc['value'] for doc in
                                                         collection_index_idx.find({}, {'value': 1})]
                                            all_ids = [doc['_id'] for doc in collection_index_idx.find({}, {'_id': 1})]
                                            for document in range(len(documents)):
                                                value = documents[document]
                                                elems = value.split("#")
                                                if len(elems) <= 1:
                                                    elems = [value]
                                                ok = 1
                                                new_val = ""
                                                for elem in elems:
                                                    if elem in ids_to_delete:
                                                        deleted = True
                                                    else:
                                                        new_val = new_val + elem + "#"
                                                new_val = new_val[:-1]
                                                if deleted:
                                                    if new_val == "":
                                                        collection_index_idx.delete_one({"_id": all_ids[document]})
                                                    else:
                                                        insert_result = collection_index_idx.update_one(
                                                            {"_id": all_ids[document]},
                                                            {"$set": {"value": new_val}},
                                                            upsert=True)
                                                    insert_result = collection_index_idx.update_one(
                                                        {"_id": all_ids[document]},
                                                        {"$set": {"value": new_val}},
                                                        upsert=True)

                                    if deleted:
                                        collection = db[table_name]
                                        for value in column_values:
                                            collection.delete_one({"_id": value})
                                        server_socket.sendto("DATA DELETED FROM TABLE {}".format(table_name).encode(),
                                                             address)
                                    else:
                                        server_socket.sendto(
                                            "Can not delete. FK constraint".format(table_name).encode(),
                                            address)

                        if not deleted:
                            server_socket.sendto("No data found with given attributes".format(table_name).encode(),
                                                 address)

            else:
                msg = "TABLE DOES NOT EXIST"
                server_socket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            server_socket.sendto(msg.encode(), address)


from prettytable import PrettyTable


def row_exists(table, row):
    for existing_row in table._rows:  # Accessing the _rows attribute to get existing rows
        if existing_row == row:
            return True
    return False


def can_use_index(index, query_attributes):
    # Iterate through each attribute in the index
    for idx_attr in index:
        # If the index attribute is in query attributes, it must match in order
        if idx_attr in query_attributes:
            # Get the position of the index attribute in query_attributes
            pos_in_query = query_attributes.index(idx_attr)
            # Check if positions match in both lists
            if index.index(idx_attr) != pos_in_query:
                return False
        else:
            # If an index attribute is not in query_attributes, break the loop
            break
    return True


operator_map = {
    '=': '$eq',
    '>': '$gt',
    '<': '$lt'
    # Add other operators as needed
}

def extract_column_names(table_name, table_structure):
    column_names = []

    # Add primary key columns first
    for pk in table_structure.get('primaryKey', []):
        column_names.append(table_name + '.' + pk['attributeName'])

    # Add foreign key columns
    for fk in table_structure.get('foreignKeys', []):
        column_names.append(table_name + '.' + fk['foreignKey'])

    # Add remaining columns
    for attr in table_structure.get('structure', []):
        if table_name+'.'+attr['attributeName'] not in column_names:
            column_names.append(table_name + '.' + attr['attributeName'])

    return column_names

def index_nested_loop_join(used_database, table_names, join_columns, indexes, table_data):
    catalog = read_json_file()

    # Extract column names for all tables
    all_column_names = []
    for table_name in table_names:
        table_structure = catalog['databases'][used_database]['tables'][table_name]
        column_names = extract_column_names(table_name, table_structure)
        all_column_names.extend(column_names)

    # Start with the data of the first table
    current_matched_rows = [[row['_id']] + row['value'].split('#')[:-1] for row in table_data[0]]
    l1 = 0
    for i in range(1, len(table_names)):
        next_table_name = table_names[i]
        next_join_column = join_columns[i - 1]
        new_matched_rows = []
        for row in current_matched_rows:
            join_value = row[l1 + 1]  # Assuming join column index matches with the row data
            matching_rows = find_matching_rows(used_database, next_table_name, join_value)
            new_matched_rows.extend(combine_rows([row], matching_rows))
        l1 = len(row)
        current_matched_rows = new_matched_rows

    # Prepend column names as the first row
    final_results = [all_column_names] + current_matched_rows

    return final_results


def buffer_rows(table, buffer_size):
    """ Generator function to yield rows in chunks. """
    buffer = []
    for row in table:  # Assuming 'table' is an iterable of rows
        buffer.append(row)
        if len(buffer) >= buffer_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


def find_row_by_id(rows, search_id):
    """ Find a row in the table where '_id' matches the search_id. """
    for row in rows:
        if row['_id'] == search_id:
            return row
    return None  # Return None if no match is found


def combine_rows(rows1, rows2):
    """ Combine two lists of rows. """
    combined = []
    for row1 in rows1:
        for row2 in rows2:
            combined_row = row1 + row2
            combined.append(combined_row)
    return combined


def find_matching_rows(used_database, table_name, join_value):
    """ Find matching rows in a table using an index for a list of rows. """
    collection = mongo_client[used_database][table_name]
    documents = collection.find({'_id': join_value})  # Assuming the join is based on a field other than '_id'

    matching_rows = []
    for doc in documents:
        row_data = [doc['_id']] + doc['value'].split('#')[:-1]
        matching_rows.append(row_data)

    return matching_rows


def hash_join(used_database, table_names, join_conditions):
    catalog = read_json_file()
    db = mongo_client[used_database]

    # Initialize hash tables for each join condition
    hash_tables = {}

    # Build Phase: Create hash tables for each join condition
    for cond in join_conditions:
        left_table, right_table = cond[0].split('.')[0], cond[1].split('.')[0]
        build_table, probe_table = (left_table, right_table) if table_names.index(left_table) < table_names.index(
            right_table) else (right_table, left_table)

        build_collection = db[build_table]
        build_rows = build_collection.find({})
        build_table_structure = catalog['databases'][used_database]['tables'][build_table]
        build_column_names = extract_column_names(build_table, build_table_structure)
        build_join_column = cond[0].split('.')[1] if build_table in cond[0] else cond[1].split('.')[1]
        build_join_column_full = f"{build_table}.{build_join_column}"
        build_join_column_index = build_column_names.index(build_join_column_full)

        hash_table = {}
        for row in build_rows:
            row_data = [row['_id']] + row['value'].split('#')[:-1]
            join_key = row_data[build_join_column_index]
            hash_key = hash(join_key)
            hash_table.setdefault(hash_key, []).append(row_data)

        hash_tables[cond] = (hash_table, build_join_column_index)

    # Initialize joined rows
    joined_rows = []

    # Probe Phase: Join tables using the hash tables
    for cond in join_conditions:
        hash_table, build_join_column_index = hash_tables[cond]
        left_table, right_table = cond[0].split('.')[0], cond[1].split('.')[0]

        probe_table = left_table if left_table != build_table else right_table
        probe_collection = db[probe_table]
        probe_rows = probe_collection.find({})
        probe_table_structure = catalog['databases'][used_database]['tables'][probe_table]
        probe_column_names = extract_column_names(probe_table, probe_table_structure)
        probe_join_column = cond[0].split('.')[1] if probe_table in cond[0] else cond[1].split('.')[1]
        probe_join_column_full = f"{probe_table}.{probe_join_column}"
        probe_join_column_index = probe_column_names.index(probe_join_column_full)

        new_joined_rows = []
        for row in probe_rows:
            row_data = [row['_id']] + row['value'].split('#')[:-1]
            join_key = row_data[probe_join_column_index]
            hash_key = hash(join_key)
            if hash_key in hash_table:
                for build_row in hash_table[hash_key]:
                    if build_row[build_join_column_index] == join_key:
                        if not joined_rows:
                            # First join condition
                            combined_row = build_row + row_data if probe_table == right_table else row_data + build_row
                            new_joined_rows.append(combined_row)
                        else:
                            # Subsequent join conditions
                            combined_row = [existing_row for existing_row in joined_rows if
                                            existing_row[:len(build_row)] == build_row]
                            if combined_row:
                                # Avoid duplicating build table rows
                                combined_row = combined_row[0] + row_data if probe_table == right_table else row_data + \
                                                                                                             combined_row[
                                                                                                                 0]
                                new_joined_rows.append(combined_row)
        joined_rows = new_joined_rows if not joined_rows else new_joined_rows

    # Extract column names for joined tables
    all_column_names = []
    for table in table_names:
        table_structure = catalog['databases'][used_database]['tables'][table]
        column_names = extract_column_names(table, table_structure)
        all_column_names.extend(column_names)

    # Prepend column names as the first row
    final_results = [all_column_names] + joined_rows
    return final_results


def execute_merge_join_query(data, used_database, query, projection_params):
    # Parse the query
    tables, join_conditions = parse_join_query(query)

    # Retrieve and externally sort the data for each table
    sorted_tables_data = {}
    for table in tables:
        join_condition = next((cond for cond in join_conditions if table in cond[0] or table in cond[1]), None)
        if not join_condition:
            continue  # Skip if no join condition found for the table
        join_column_index = get_join_column_indices(used_database, table, join_condition)
        sorted_tables_data[table] = external_sort(table, join_column_index, chunk_size=1000)

    # Initialize result with the first table's sorted data
    result = sorted_tables_data[tables[0]]

    # Perform sort-merge join for each subsequent table
    for i in range(1, len(tables)):
        if i == 1:
            result_columns_old = extract_column_names(tables[i-1], data['databases'][used_database]['tables'][tables[i-1]])
        else:
            result_columns_old = final_columns_names
        result_columns = extract_column_names(tables[i], data['databases'][used_database]['tables'][tables[i]])

        next_table = tables[i]
        next_table_structure = data['databases'][used_database]['tables'][next_table]
        next_table_join_column_index = get_join_column_index_for_next_table(result_columns, next_table, join_conditions[i - 1])
        result_join_column_index = get_join_column_index_for_result(result_columns_old, join_conditions[i - 1])

        result = sort_merge_join_with_result(result, sorted_tables_data[next_table], result_join_column_index,
                                             next_table_join_column_index)
        # Update result_columns to include the next table's columns
        new_columns = extract_column_names(next_table, next_table_structure)
        final_columns_names = result_columns_old + new_columns
        result_columns = update_result_columns(result_columns, new_columns, join_conditions[i - 1])

    result = [final_columns_names] + result
    return result

# Implement the update_result_columns function to correctly update result_columns after each join
def update_result_columns(current_columns, new_columns, join_condition):
    # Logic to merge current_columns and new_columns based on the join condition
    # Ensure that the join columns are not duplicated in the result columns
    updated_columns = current_columns[:]
    for col in new_columns:
        if col not in updated_columns:
            updated_columns.append(col)
    return updated_columns



def execute_join_query(data, used_database, query, projection_params):

    # Parse the query
    tables, join_conditions = parse_join_query(query)

    # Retrieve table data and prepare join columns
    table_data = [retrieve_table_data(table) for table in tables]
    join_columns = [condition[0].split('.')[1] for condition in join_conditions]  # Assuming 'table.column' format

    # Prepare indexes (this is a simplified example)
    indexes = prepare_indexes(data, used_database, tables,
                              join_columns)  # Implement this based on your index structure

    # Execute index nested loop join
    results = index_nested_loop_join(used_database, tables, join_columns, indexes, table_data)
    # results = execute_merge_join_query(data,used_database,query,projection_params)
    # results = hash_join(used_database, tables, join_conditions)

    # Filter column names based on projection parameters
    if projection_params:
        column_names = [col for col in results[0] if col in projection_params]
    else:
        column_names = results[0]

    # Create PrettyTable with specified columns
    output_table = PrettyTable(column_names)

    # Iterate over the results and project only the required columns
    for row in results[1:]:
        if projection_params:
            # Project only the columns specified in projection_params
            projected_row = [row[results[0].index(col)] for col in column_names]
            output_table.add_row(projected_row)
        else:
            output_table.add_row(row)

    # Write the PrettyTable to select.txt
    with open("databases/select.txt", 'w') as f:
        f.write(output_table.get_string())

    return "SELECT result written to select.txt"


def sort_dataset_on_column(dataset, column_index):
    """
    Sorts a dataset (list of lists) based on the specified column index.
    """
    return sorted(dataset, key=lambda x: x[column_index])


def sort_table_on_column(table_data, column_index, chunk_size):
    """
    Sorts a table (list of lists) based on the specified column index.
    Applies external sorting if the dataset is large.
    """
    # If the data fits into a single chunk, sort it directly
    if len(table_data) <= chunk_size:
        return sorted(table_data, key=lambda x: x[column_index])

    # For larger datasets, apply external sorting
    sorted_chunks = []
    for i in range(0, len(table_data), chunk_size):
        chunk = table_data[i:i + chunk_size]
        sorted_chunk = sorted(chunk, key=lambda x: x[column_index])
        sorted_chunks.append(sorted_chunk)

    # Merge the sorted chunks
    return merge_chunks(sorted_chunks)

def merge_chunks(sorted_chunks, sort_column_index):
    # This function merges sorted chunks of data
    merged_data = [item for chunk in sorted_chunks for item in chunk]
    merged_data.sort(key=lambda x: x[sort_column_index])

    return merged_data

def external_sort(table_name, sort_column_index, chunk_size):
    # This function sorts the table based on the sort_column_index
    # using external sorting and returns sorted chunks

    db = mongo_client[used_database]
    collection = db[table_name]
    rows = collection.find({})

    # Break the data into chunks and sort each chunk
    sorted_chunks = []
    chunk = []
    for row in rows:
        chunk.append([row['_id']] + row['value'].split('#')[:-1])
        if len(chunk) == chunk_size:
            chunk.sort(key=lambda x: x[sort_column_index])
            sorted_chunks.append(chunk)
            chunk = []

    # Sort the last chunk if it's not empty
    if chunk:
        chunk.sort(key=lambda x: x[sort_column_index])
        sorted_chunks.append(chunk)

    # Merging chunks
    sorted_data = merge_chunks(sorted_chunks, sort_column_index)
    return sorted_data

def get_join_column_indices(database_name, table_name, join_condition):
    catalog = read_json_file()

    # Determine the relevant part of the join condition for the specified table
    join_column = join_condition[0].split('.')[1] if table_name == join_condition[0].split('.')[0] \
        else join_condition[1].split('.')[1] if table_name == join_condition[1].split('.')[0] \
        else None
    if not join_column:
        raise ValueError(f"Join column for table {table_name} not found in join condition {join_condition}")

    # Extract table structure from the catalog
    table_info = catalog['databases'][database_name]['tables'][table_name]
    table_structure = table_info['structure']
    primary_keys = [pk['attributeName'] for pk in table_info.get('primaryKey', [])]
    foreign_keys = [fk['foreignKey'] for fk in table_info.get('foreignKeys', [])]

    # Generate the column order list
    column_order = primary_keys + foreign_keys
    for attr in table_structure:
        if attr['attributeName'] not in column_order:
            column_order.append(attr['attributeName'])

    # Find the index of the join column in the order list
    if join_column in column_order:
        return column_order.index(join_column)

    raise ValueError(f"Column {join_column} not found in table {table_name}")


def get_join_column_index_for_next_table(result_columns, next_table, join_condition):
    # Extract the join column from the join condition
    join_column = join_condition[1].split('.')[1] if next_table in join_condition[1] else join_condition[0].split('.')[1]

    # Construct the fully qualified column name (table.column)
    qualified_join_column = f"{next_table}.{join_column}"

    # Find the index of the join column in the result columns
    if qualified_join_column in result_columns:
        join_column_index = result_columns.index(qualified_join_column)
    else:
        raise ValueError(f"Column {qualified_join_column} not found in the result set")

    return join_column_index


def sort_merge_join_with_result(result, next_table, result_join_column_index, next_table_join_column_index):
    # Sort the next table and the result set if they are not already sorted on the join column
    sorted_result = sort_dataset_on_column(result, result_join_column_index)
    sorted_next_table = sort_table_on_column(next_table, next_table_join_column_index, 100)

    # Perform the merge join
    merged_result = []
    result_index, next_table_index = 0, 0

    while result_index < len(sorted_result) and next_table_index < len(sorted_next_table):
        result_row = sorted_result[result_index]
        next_table_row = sorted_next_table[next_table_index]

        if result_row[result_join_column_index] == next_table_row[next_table_join_column_index]:
            merged_result.append(result_row + next_table_row)
            result_index += 1
        elif result_row[result_join_column_index] < next_table_row[next_table_join_column_index]:
            result_index += 1
        else:
            next_table_index += 1
    return merged_result

def get_join_column_index_for_result(result_columns, join_condition):
    # Split the join condition to identify the table and column names
    left_table, left_column = join_condition[0].split('.')
    right_table, right_column = join_condition[1].split('.')

    # Determine which part of the join condition refers to a column in the result_columns
    qualified_join_column = None
    if f"{left_table}.{left_column}" in result_columns:
        qualified_join_column = f"{left_table}.{left_column}"
    elif f"{right_table}.{right_column}" in result_columns:
        qualified_join_column = f"{right_table}.{right_column}"

    # Check if the qualified column name is in the result_columns and find its index
    if qualified_join_column and qualified_join_column in result_columns:
        join_column_index = result_columns.index(qualified_join_column)
    else:
        raise ValueError(f"Column {qualified_join_column} not found in the result set")

    return join_column_index

def retrieve_table_data(table_name):
    """
    Retrieve data for a given table. This could involve querying your database
    or reading from a file. Returns a list of rows, where each row is a dictionary.
    """
    # Example of fetching data from a MongoDB collection
    collection = mongo_client[used_database][table_name]
    return list(collection.find({}))


def prepare_indexes(data, used_database, tables, join_columns):
    """
    Prepare a dictionary of indexes. The key is 'table_column' and the value is the index data.
    """
    indexes = {}
    for table in tables:
        table_indexes = data["databases"][used_database]["tables"][table]["indexFiles"]
        for inx in table_indexes:
            if isinstance(inx['indexAttributes'], dict):
                inx['indexAttributes'] = [inx['indexAttributes']]
            for ind in inx['indexAttributes']:
                attribute_name = ind['attributeName']
                if attribute_name in join_columns:
                    index_file_name = inx['indexName']
                    indexes[f"{table}_foreignKey{attribute_name}.ind"] = get_index_data(used_database, index_file_name)
    return indexes


def get_index_data(used_database, collection_name):
    """
    Retrieve index data from a MongoDB collection. The collection name should be the full name
    including the database (e.g., 'mydb.myIndexCollection').
    """
    index_data = {}
    client = MongoClient(uri, server_api=ServerApi('1'))

    db = mongo_client[used_database]
    collection = db[collection_name]

    try:
        for document in collection.find():
            index_data[document['_id']] = document['value']
    except Exception as e:
        print(f"An error occurred while fetching index data: {e}")
        return None
    finally:
        client.close()  # Ensure that the client is closed after operation

    return index_data


def parse_join_query(query):
    query = ' '.join(query)
    # Regular expression to match table names and join conditions
    # It assumes the query structure is well-formed as per the example
    pattern = r'FROM (\w+)|JOIN (\w+) ON (\w+\.\w+) = (\w+\.\w+)'

    matches = re.findall(pattern, query)

    if not matches:
        return [], []

    # Extracting the first table name
    tables = [matches[0][0]]

    # Extracting join tables and conditions
    join_conditions = []
    for _, table, left_col, right_col in matches:
        if table:
            tables.append(table)
            join_conditions.append((left_col, right_col))

    return tables, join_conditions

def extract_group_by_fields(params):
    """
    Extracts the fields used in the GROUP BY clause from the query parameters.
    :param params: List of words in the SQL-like query.
    :return: A list containing the fields used for GROUP BY.
    """
    if 'GROUP BY' in ' '.join(params).upper():
        # Find the index of 'GROUP BY' in the query
        group_by_index = params.index('GROUP') + 1

        # Extract fields after 'GROUP BY'
        group_by_fields = params[group_by_index + 1:]

        # Assuming fields are separated by commas, split them
        # Here, we join the list to handle spaces and then split by comma
        group_by_fields = ' '.join(group_by_fields).split(',')

        # Trim spaces and return the fields
        res = [field.strip() for field in group_by_fields]
        return [res[0].split(' ')[0]]
    else:
        # Return an empty list if 'GROUP BY' is not found
        return []

def parse_select_clause(select_clause):
    """
    Parses the SELECT clause to identify the aggregation function and field.
    :param select_clause: The SELECT clause of the query.
    :return: A tuple (aggregation function, field).
    """
    # Assume the format is "AGG_FUNC(FIELD)" e.g., "COUNT(GroupID)"
    parts = select_clause.replace(')', '').split('(')
    agg_func = parts[0].strip()
    agg_field = parts[1].strip()

    return agg_func, agg_field

def full_table_aggregation(db_name, table_name, collection, select_fields, group_by_fields, catalog):
    aggregation_results = {}

    agg_funcs, agg_fields = parse_select_fields(select_fields)

    group_by_field = group_by_fields[0]
    group_by_field_index = find_field_index(db_name, table_name, group_by_field, catalog)

    for doc in collection.find():
        values = doc['value'].split('#')
        if group_by_field_index < len(values):
            group_value = values[group_by_field_index]

            if group_value not in aggregation_results:
                aggregation_results[group_value] = [0] * len(agg_fields)

            for i, (agg_func, agg_field) in enumerate(zip(agg_funcs, agg_fields)):
                field_index = find_field_index(db_name, table_name, agg_field, catalog)
                if field_index < len(values):
                    field_value = values[field_index]
                    if is_numeric_type(field_value):
                        field_value = float(field_value)

                    if agg_func == 'COUNT':
                        aggregation_results[group_value][i] += 1
                    elif agg_func == 'SUM':
                        aggregation_results[group_value][i] += field_value
                    elif agg_func == 'AVG':
                        total, count = aggregation_results[group_value][i]
                        aggregation_results[group_value][i] = (total + field_value, count + 1)

    for group_value, results in aggregation_results.items():
        for i, (agg_func, agg_field) in enumerate(zip(agg_funcs, agg_fields)):
            if agg_func == 'AVG':
                total, count = results[i]
                aggregation_results[group_value][i] = total / count if count > 0 else 0

    return aggregation_results

def find_field_index(used_database, table_name, field, catalog):
    """
    Find the index of a field in the record based on the catalog.
    :param field: The field name.
    :param catalog: The catalog of the database schema.
    :return: The index of the field in the record.
    """
    print(catalog["databases"][used_database]["tables"][table_name]["structure"])
    i = 0
    for index in catalog["databases"][used_database]["tables"][table_name]["structure"]:
        if field == index['attributeName']:
            print(i)
            return i
        i=i+1
    return -1

def is_numeric_type(value):
    """
    Check if a value is of a numeric type.
    :param value: The value to check.
    :return: True if the value is numeric, False otherwise.
    """
    try:
        float(value)
        return True
    except ValueError:
        return False


def aggregate_data_with_index(db_name, table_name, select_fields, group_by_fields, index_info, catalog):
    """
    Performs aggregation on MongoDB documents using indexes where possible.
    :param db_name: Name of the database.
    :param table_name: Name of the table (collection in MongoDB).
    :param select_fields: The fields in the SELECT clause of the query.
    :param group_by_fields: Fields to group by (extracted from the GROUP BY clause).
    :param index_info: Information about the indexes available.
    :return: Aggregated data.
    """
    db = mongo_client[db_name]
    collection = db[table_name]

    # Check if an index exists for the GROUP BY field
    index_exists, index_collection_name = check_index(group_by_fields[0], index_info)

    if index_exists:
        # Use index for aggregation
        indexed_collection = db[index_collection_name]
        aggregated_data = use_index_for_aggregation(db_name, table_name, indexed_collection, select_fields, group_by_fields, catalog)
    else:
        # Full table scan for aggregation
        aggregated_data = full_table_aggregation(db_name, table_name, collection, select_fields, group_by_fields, catalog)

    return aggregated_data


def check_index(group_by_field, index_info):
    """
    Checks if an index exists for the given field.
    :param group_by_field: The field to check for an index.
    :param index_info: Information about the indexes available.
    :return: Tuple of (index_exists, index_collection_name).
    """
    for index in index_info:
        print(index['indexAttributes'])
        if group_by_field in [attr['attributeName'] for attr in index['indexAttributes']]:
            return True, index['indexName']
    return False, None

def parse_select_clause(select_clause):
    """
    Parses the SELECT clause to identify the aggregation function and field.
    :param select_clause: The SELECT clause of the query.
    :return: A tuple (aggregation function, field).
    """
    # Assume the format is "AGG_FUNC(FIELD)" e.g., "COUNT(GroupID)"
    parts = select_clause.replace(')', '').split('(')
    agg_func = parts[0].strip()
    agg_field = parts[1].strip()

    return agg_func, agg_field
def use_index_for_aggregation(db_name, table_name, indexed_collection, select_fields, group_by_fields, catalog):
    """
    Uses an index to perform aggregation.
    :param db_name: The name of the database.
    :param table_name: The name of the table (collection in MongoDB).
    :param indexed_collection: The indexed collection in MongoDB.
    :param select_fields: The fields in the SELECT clause of the query.
    :param group_by_fields: Fields to group by.
    :param catalog: The catalog of the database schema.
    :return: Aggregated data using the index.
    """
    db = mongo_client[db_name]
    main_collection = db[table_name]

    # Initialize a dictionary to store aggregation results
    aggregation_results = {}

    # Parse select fields for aggregation functions and fields
    agg_funcs, agg_fields = parse_select_fields(select_fields)

    # Query the index collection to get all documents
    index_documents = indexed_collection.find()

    # Iterate over the index documents and perform the aggregation
    for index_doc in index_documents:
        group_values = index_doc['_id']  # The values for the group by fields
        doc_ids = index_doc['value'].split('#')  # The list of document IDs

        # Initialize the aggregation value
        result_row = []
        for agg_func, agg_field in zip(agg_funcs, agg_fields):
            if agg_func == 'COUNT':
                agg_result = len(doc_ids)
                result_row.append(agg_result)
            elif agg_func in ['SUM', 'AVG', 'MIN', 'MAX']:
                agg_result = compute_aggregation(agg_func, agg_field, doc_ids, main_collection, catalog["databases"][db_name]["tables"][table_name])
                result_row.append(agg_result)
            else:
                # For non-aggregated fields, just add the group value
                result_row.append(group_values)
        aggregation_results[result_row[0]] = result_row[1]
    return aggregation_results

def parse_select_fields(select_fields):
    """
    Parses the SELECT fields to identify any aggregation functions and fields.
    :param select_fields: The fields in the SELECT clause of the query.
    :return: A tuple of lists (aggregation functions, fields).
    """
    agg_funcs = []
    agg_fields = []

    for field in select_fields:
        if '(' in field and ')' in field:  # Check if it's an aggregation function
            parts = field.replace(')', '').split('(')
            agg_funcs.append(parts[0].strip())
            agg_fields.append(parts[1].strip())
        else:
            agg_funcs.append(None)
            agg_fields.append(field.strip())

    return agg_funcs, agg_fields

def extract_having_clause(params):
    """
    Extracts the condition used in the HAVING clause from the query parameters.
    :param params: List of words in the SQL-like query.
    :return: A tuple containing the field, operator, and value for the HAVING condition.
    """
    if 'HAVING' in ' '.join(params).upper():
        # Find the index of 'HAVING' in the query
        having_index = params.index('HAVING') + 1

        # Extract condition after 'HAVING'
        having_condition = params[having_index:]

        # Assuming the condition is in the format "field operator value"
        field, operator, value = having_condition[0], having_condition[1], having_condition[2]

        return field, operator, value
    else:
        # Return None if 'HAVING' is not found
        return None, None, None


def compute_aggregation(agg_func, agg_field, doc_ids, main_collection, catalog_structure):
    """
    Computes the aggregation (SUM, AVG, MIN, MAX) for a set of document IDs.
    :param agg_func: The aggregation function (SUM, AVG, MIN, MAX).
    :param agg_field: The field to aggregate on.
    :param doc_ids: List of document IDs to aggregate over.
    :param main_collection: The main MongoDB collection containing the documents.
    :param catalog_structure: The structure of the table from the catalog.
    :return: The aggregation result.
    """

    field_position = -1
    for i, struct in enumerate(catalog_structure["structure"]):
        if struct['attributeName'] == agg_field:
            field_position = i

    if field_position == -1:
        raise ValueError(f"Field {agg_field} not found in catalog structure")

    # Fetch the actual documents based on doc_ids
    actual_docs = []
    for id in doc_ids:
        actual_docs.append([doc['value'] for doc in main_collection.find({'_id': id}, {'value': 1})])

    # Extract the relevant field values for aggregation
    field_values = []
    for doc in actual_docs:
        values = doc[0].split('#')
        if field_position < len(values):
            try:
                field_value = int(values[field_position]) if catalog_structure["structure"][field_position]['type'] == 'int' else values[field_position]
                field_values.append(field_value)
            except ValueError:
                # Handle cases where conversion to int fails
                continue

    # Perform the aggregation based on agg_func
    if agg_func == 'SUM':
        aggregated_value = sum(field_values)
    elif agg_func == 'AVG':
        aggregated_value = sum(field_values) / len(field_values) if field_values else 0
    elif agg_func == 'MIN':
        aggregated_value = min(field_values) if field_values else None
    elif agg_func == 'MAX':
        aggregated_value = max(field_values) if field_values else None

    return aggregated_value

def filter_having_data(aggregated_data, having_field, having_operator, having_value):
    """
    Filters aggregated data based on the HAVING clause condition.
    :param aggregated_data: The aggregated data as a dictionary.
    :param having_field: The field to apply the HAVING condition to.
    :param having_operator: The operator in the HAVING condition (e.g., '=', '<', '>').
    :param having_value: The value to compare against in the HAVING condition.
    :return: Filtered aggregated data.
    """
    filtered_data = {}

    # Convert having_value to the appropriate type (int or float) if it's numeric
    if having_value.isdigit():
        having_value = int(having_value)
    elif having_value.replace('.', '', 1).isdigit():
        having_value = float(having_value)

    for group_value, agg_result in aggregated_data.items():
        # Convert group_value to the appropriate type (int or float) if it's numeric
        group_value_converted = group_value
        if group_value.isdigit():
            group_value_converted = int(group_value)
        elif group_value.replace('.', '', 1).isdigit():
            group_value_converted = float(group_value)

        # Apply the HAVING condition
        if having_operator == '=' and group_value_converted == having_value:
            filtered_data[group_value] = agg_result
        elif having_operator == '<' and group_value_converted < having_value:
            filtered_data[group_value] = agg_result
        elif having_operator == '>' and group_value_converted > having_value:
            filtered_data[group_value] = agg_result

    return filtered_data


def select(params):
    data = read_json_file()
    global used_database
    if "from" not in " ".join(params).lower():
        serverSocket.sendto("INVALID SELECT COMMAND".encode(), address)
    else:
        if used_database:
            search_table = " ".join(params)
            table_s = search_table[search_table.index("FROM") + 5:].split(" ")[0]
            table = data["databases"][used_database]["tables"].get(table_s, None)
            table_name = table["tableName"]
            if table:
                db = mongo_client[used_database]
                collection = db[table["tableName"]]

                # Retrieve all documents from the collection
                documents = list(collection.find({}))

                st_copy = " ".join(params)
                select_type = ''
                params_projection = []
                if params[0] != '*':
                    params_projection = params[:params.index("FROM")]
                    # params_projection = "".join(params_projection).split(" ")
                    if params_projection[0] == 'distinct':
                        select_type = 'distinct'
                        params_projection = params_projection[1:]
                        params = params[1:]
                    params_projection = params_projection[0].split(",")
                    t = PrettyTable(params_projection)

                if len(params) == 3:
                    attributes = []
                    for str in data["databases"][used_database]["tables"][params[2]]["structure"]:
                        attributes.append(str["attributeName"])
                    if len(params_projection) > 0:
                        table = PrettyTable(params_projection)
                    else:
                        table = PrettyTable(attributes)

                    attributes_mongo = [field for field in
                                        collection.find_one()]  # Get field names from the first document
                    for doc in documents:
                        values = [doc.get(attr, "") for attr in attributes_mongo]
                        values_table = []
                        values_table.append(values[0])
                        values_table += values[1].split('#')
                        values_table = values_table[:-1]
                        selected_values = []
                        if len(params_projection) > 0:
                            for attr in range(len(attributes)):
                                if attributes[attr] in params_projection:
                                    selected_values.append(values_table[attr])
                            table.add_row(selected_values)
                        else:
                            table.add_row(values_table)

                    with open("databases/select.txt", 'w') as sFile:
                        sFile.write(table.get_string())

                    server_socket.sendto("SELECT result written to select.txt".encode(), address)
                if len(params) > 3:
                    # Check if GROUP BY is in the query
                    if 'GROUP' in params:
                        # Extract select and group by fields
                        select_fields = params[:params.index("FROM")]

                        # Extract the GROUP BY fields
                        group_by_fields = extract_group_by_fields(params)

                        # Extract HAVING clause if present
                        having_field, having_operator, having_value = extract_having_clause(params)


                        # Aggregate data based on GROUP BY fields
                        aggregated_data = aggregate_data_with_index(used_database, table_name, select_fields, group_by_fields,
                                                                    data["databases"][used_database]["tables"][table_name]["indexFiles"], data)

                        # Filter data based on HAVING clause
                        if having_field:
                            print(aggregated_data)
                            filtered_data = filter_having_data(aggregated_data, having_field, having_operator,
                                                               having_value)
                            print(filtered_data)

                            # Create and populate PrettyTable
                            headers = [field.strip() for field in select_fields[0].split(",")]
                            table = PrettyTable(headers)

                            for key, value in filtered_data.items():
                                row = [key] if len(headers) == 1 else [key, value]
                                table.add_row(row)

                            print(table)
                            sFile = open("databases/select.txt", 'w')
                            sFile.write(table.get_string())
                            sFile.close()
                            server_socket.sendto("SELECT result written to select.txt".encode(), address)
                        else:
                            # Create and populate PrettyTable
                            headers = [field.strip() for field in select_fields[0].split(",")]
                            table = PrettyTable(headers)

                            for key, value in aggregated_data.items():
                                row = [key] if len(headers) == 1 else [key, value]
                                table.add_row(row)

                            print(table)
                            sFile = open("databases/select.txt", 'w')
                            sFile.write(table.get_string())
                            sFile.close()
                            server_socket.sendto("SELECT result written to select.txt".encode(), address)

                    # Handle cases where there is no GROUP BY
                    else:
                        pass
                    # if 'JOIN' in params:
                    #     res = execute_join_query(data, used_database, params, params_projection)
                    #     server_socket.sendto(res.encode(), address)
                    # if 'WHERE' in params:
                    #     attributes = []
                    #     selection = []
                    #     # select_type = params[0].lower()
                    #     table = params[2]
                    #     for str in data["databases"][used_database]["tables"][table]["structure"]:
                    #         attributes.append(str["attributeName"])
                    #     if len(params_projection) > 0:
                    #         t = PrettyTable(params_projection)
                    #     else:
                    #         t = PrettyTable(attributes)
                    #     params = params[4:]
                    #     if "and" in params:
                    #         params.pop(1)
                    #     index = data["databases"][used_database]["tables"][table]["indexFiles"]
                    #     arg_attributes = []
                    #     arg_values = []
                    #     copy_arg_attributes = []
                    #     copy_arg_values = []
                    #     indexes = []
                    #     arg_signs = []
                    #     for s in params:
                    #         split_result = re.split('([=><]|LIKE)', s)
                    #         if len(split_result) >= 3:
                    #             arg_attributes.append(split_result[0].strip())
                    #             arg_signs.append(split_result[1].strip())
                    #             arg_values.append(split_result[2].strip())
                    #             copy_arg_attributes.append(split_result[0].strip())
                    #             copy_arg_values.append(split_result[2].strip())
                    #     index_file_names = []
                    #     for inx in index:
                    #         index_file_names.append(inx['indexName'])
                    #         if isinstance(inx['indexAttributes'], dict):
                    #             inx['indexAttributes'] = [inx['indexAttributes']]
                    #         l = []
                    #         for ind in inx['indexAttributes']:
                    #             l.append(ind['attributeName'])
                    #         indexes.append(l)
                    #     counter = 0
                    #     # a) If there are index files on more than one attribute from WHERE use every index file.
                    #     index_exists = False
                    #     for i in indexes:
                    #         if set(i).issubset(set(arg_attributes)):
                    #             print("Index used on")
                    #             print(i)
                    #             print("---------------------------")
                    #             params_values = ""
                    #             query = {}
                    #             for at in i:
                    #                 x = arg_attributes.index(at)
                    #                 params_values += arg_values[x] + ":"
                    #                 if arg_signs[x] == '=':  # Direct equality
                    #                     query["_id"] = arg_values[x]
                    #                 elif arg_signs[x] == 'LIKE':  # LIKE operator
                    #                     # Convert the SQL LIKE pattern to a regular expression
                    #                     regex_pattern = '^' + arg_values[x].replace('*', '.*') + '$'
                    #                     query["_id"] = {'$regex': regex_pattern}
                    #                 else:  # Other comparisons
                    #                     mongo_operator = operator_map.get(arg_signs[x])
                    #                     if mongo_operator:
                    #                         query["_id"] = {mongo_operator: arg_values[x]}
                    #             params_values = params_values[:-1]
                    #             collection = db[index_file_names[counter]]
                    #
                    #             i_values_cursor = collection.find(query)
                    #             # i_values = collection.find_one({"_id": params_values})
                    #             for i_value in i_values_cursor:
                    #                 if 'value' in i_value and i_value['value'][-1] != '#':
                    #                     i_value['value'] = i_value['value'] + '#'
                    #                     i_values = i_value['value'].split("#")
                    #                     for val in i_values:
                    #                         l = []
                    #                         collection = db[table_name]
                    #                         result = collection.find_one({"_id": val})
                    #                         if result and result['value'] != None:
                    #                             if result['value'][-1] != '#':
                    #                                 result['value'] = result['value'] + '#'
                    #                             obj = result['value'].split("#")
                    #                             l.append(val)
                    #                             for o in obj:
                    #                                 l.append(o)
                    #                             selection.append(l)
                    #                             index_exists = True
                    #                     old_arg_attributes = arg_attributes
                    #                     arg_attributes = list(set(arg_attributes) - set(i))
                    #                     for j in range(len(old_arg_attributes)):
                    #                         if old_arg_attributes[j] not in arg_attributes:
                    #                             arg_values.remove(arg_values[j])
                    #         else:
                    #             # b) and d) If there are composite index files, use them on prefix.
                    #             if can_use_index(i, arg_attributes):
                    #
                    #                 query = {}
                    #                 params_values = ""
                    #                 for at in arg_attributes:
                    #                     if at in i:
                    #                         print("Index used on")
                    #                         print(i)
                    #                         print("---------------------------")
                    #                         x = i.index(at)
                    #                         x -= 1
                    #                         params_values += arg_values[x] + ":"
                    #                         if arg_signs[x] == '=':  # Direct equality
                    #                             query["_id"] = arg_values[x]
                    #                         elif arg_signs[x] == 'LIKE':  # LIKE operator
                    #                             # Convert the SQL LIKE pattern to a regular expression
                    #                             regex_pattern = '^' + arg_values[x].replace('*', '.*') + '$'
                    #                             query["_id"] = {'$regex': regex_pattern}
                    #                         else:  # Other comparisons
                    #                             mongo_operator = operator_map.get(arg_signs[x])
                    #                             if mongo_operator:
                    #                                 query["_id"] = {mongo_operator: arg_values[x]}
                    #                 params_values = params_values[:-1]
                    #                 collection = db[index_file_names[counter]]
                    #                 i_values_cursor = collection.find(query)
                    #                 for i_value in i_values_cursor:
                    #                     if 'value' in i_value and i_value['value'][-1] != '#':
                    #                         i_value['value'] = i_value['value'] + '#'
                    #                         i_values = i_value['value'].split("#")
                    #                         for val in i_values:
                    #                             l = []
                    #                             collection = db[table_name]
                    #                             result = collection.find_one({"_id": val})
                    #                             if result and result['value'] != None:
                    #                                 if result['value'][-1] != '#':
                    #                                     result['value'] = result['value'] + '#'
                    #                                 obj = result['value'].split("#")
                    #                                 l.append(val)
                    #                                 for o in obj:
                    #                                     l.append(o)
                    #                                 selection.append(l)
                    #                                 index_exists = True
                    #                     old_arg_attributes = arg_attributes
                    #                     arg_attributes = list(set(arg_attributes) - set(i))
                    #                     for j in range(len(old_arg_attributes)):
                    #                         if old_arg_attributes[j] not in arg_attributes:
                    #                             arg_values.remove(arg_values[j])
                    #
                    #         counter += 1
                    #     if not index_exists:
                    #         # table scan
                    #         attributes = []
                    #         for str in data["databases"][used_database]["tables"][table_name]["structure"]:
                    #             attributes.append(str["attributeName"])
                    #         if len(params_projection) > 0:
                    #             table = PrettyTable(params_projection)
                    #         else:
                    #             table = PrettyTable(attributes)
                    #
                    #         attributes_mongo = [field for field in
                    #                             collection.find_one()]  # Get field names from the first document
                    #         for doc in documents:
                    #             values = [doc.get(attr, "") for attr in attributes_mongo]
                    #             values_table = []
                    #             values_table.append(values[0])
                    #             values_table += values[1].split('#')
                    #             values_table = values_table[:-1]
                    #             selection.append(values_table)
                    #     if select_type == "distinct":
                    #         attributes = []
                    #         for str in data["databases"][used_database]["tables"][table_name]["structure"]:
                    #             attributes.append(str["attributeName"])
                    #         result = []
                    #         for s in selection:
                    #             if s[-1] == '':
                    #                 s = s[:-1]
                    #             k = 0
                    #             c = 0
                    #             j = 0
                    #             for atr in attributes:
                    #                 if atr in copy_arg_attributes:
                    #                     if arg_signs[j] == '=':
                    #                         if s[k] == copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == '>':
                    #                         if s[k] > copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == '<':
                    #                         if s[k] < copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == 'LIKE':
                    #                         # Convert the LIKE pattern to a regular expression
                    #                         pattern = '^' + copy_arg_values[j].replace('*', '.*') + '$'
                    #                         if re.match(pattern, s[k]):
                    #                             c += 1
                    #                             j += 1
                    #                 k += 1
                    #             if c == len(copy_arg_attributes):
                    #                 selected_values = []
                    #                 if len(params_projection) > 0:
                    #                     for attr in range(len(attributes)):
                    #                         if attributes[attr] in params_projection:
                    #                             selected_values.append(s[attr])
                    #                     ids = [j[0] for j in result]
                    #                     if s[0] not in ids:
                    #                         if not row_exists(t, selected_values):
                    #                             t.add_row(selected_values)
                    #                             result.append(s)
                    #
                    #                 else:
                    #                     ids = [j[0] for j in result]
                    #                     if s[0] not in ids:
                    #                         if not row_exists(t, s):
                    #                             t.add_row(s)
                    #                             result.append(s)
                    #     else:
                    #         attributes = []
                    #         for str in data["databases"][used_database]["tables"][table_name]["structure"]:
                    #             attributes.append(str["attributeName"])
                    #         result = []
                    #         for s in selection:
                    #             if s[-1] == '':
                    #                 s = s[:-1]
                    #             k = 0
                    #             c = 0
                    #             j = 0
                    #             for atr in attributes:
                    #                 if atr in copy_arg_attributes:
                    #                     if arg_signs[j] == '=':
                    #                         if s[k] == copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == '>':
                    #                         if s[k] > copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == '<':
                    #                         if s[k] < copy_arg_values[j]:
                    #                             c += 1
                    #                             j += 1
                    #                     elif arg_signs[j] == 'LIKE':
                    #                         # Convert the LIKE pattern to a regular expression
                    #                         pattern = '^' + copy_arg_values[j].replace('*', '.*') + '$'
                    #                         if re.match(pattern, s[k]):
                    #                             c += 1
                    #                             j += 1
                    #
                    #                 k += 1
                    #
                    #             if c == len(copy_arg_attributes):
                    #                 selected_values = []
                    #                 if len(params_projection) > 0:
                    #                     for attr in range(len(attributes)):
                    #                         if attributes[attr] in params_projection:
                    #                             selected_values.append(s[attr])
                    #                     ids = [j[0] for j in result]
                    #                     if s[0] not in ids:
                    #                         t.add_row(selected_values)
                    #                         result.append(s)
                    #
                    #                 else:
                    #                     ids = [j[0] for j in result]
                    #                     if s[0] not in ids:
                    #                         t.add_row(s)
                    #                         result.append(s)
                    #
                    #     sFile = open("databases/select.txt", 'w')
                    #     sFile.write(t.get_string())
                    #     sFile.close()
                    #     server_socket.sendto("SELECT result written to select.txt".encode(), address)
            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


import random

import random


def generate(num_records):
    group_ids = [242, 243, 244, 245, 246, 247, 248]
    marks_range = range(1, 10)
    num_records = int(num_records[0])
    records = []
    for stud_id in range(1, num_records + 1):
        group_id = random.choice(group_ids)
        name = f"Student{stud_id}"
        tel = f"tel{stud_id}"
        mark = random.choice(marks_range)
        record = ['into', 'students3', '(StudID,', 'GroupID,', 'Name,', 'Tel,', 'mark)', 'values',
                  f"({stud_id},{group_id},'{name}','{tel}',{mark});"]
        # record = ['into', 'students2', '(StudID,', 'GroupID,', 'Name,', 'Tel,', 'mark)', 'values',
        #           f"({stud_id},{group_id},'{name}','{tel}',{mark});"]
        insert(record)
    server_socket.sendto("Generated data".encode(), address)
    return True


print("Waiting for client...")
while True:
    client_cmd, address = server_socket.recvfrom(bufferSize)
    client_cmd = client_cmd.decode().split(" ")
    if client_cmd[0].lower() in ["create", "drop", "use", "insert", "delete", "select", "generate"]:
        func = locals()[client_cmd[0].lower()]
        del client_cmd[0]
        func(client_cmd)
    elif client_cmd[0].lower() == "exit":
        server_socket.sendto("Client closed".encode(), address)
    else:
        server_socket.sendto("Invalid command".encode(), address)
