import json
import sys
import os, shutil
import boto3
import collections

tempDir = '../Temp/'
infraPath = '../Templates/'

# Removes a legacy folder and files then creates it
def createFolder(path):

    # Remove the existing content and folder
    if os.path.isdir(path):
        shutil.rmtree(path)

    # Create the new folder
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed\n" % path)
    else:
        print ("Successfully created the directory %s \n" % path)

# Uses a glueDBgeneral.template and injects function config into it.
def generateDatabaseTemplate(fileToCopy, fileToSave, config):

    # Creating python destination directory and .template file
    global infraPath
    databaseName = os.path.splitext(fileToSave)[0]
    global tempDir

    # Open the glueTablegeneral.template generic file
    with open(fileToCopy) as json_file:
        data = json.load(json_file)

    # Load the downloaded function function.template
    with open(tempDir + databaseName) as json_file:
        databaseConfig = json.load(json_file)

    # Open the downloaded function.template
    # Edit the parameters, if keys exist we append to current values
    for key, value in databaseConfig["GLUE"].items():
        if key == "Name":
            data["Parameters"]["Name"]["Default"] = value
        elif key == "Description":
            data["Parameters"]["Description"]["Default"] = value
        elif key == "Parameters":
            data["Resources"]["GlueDatabase"]["Properties"]["DatabaseInput"]["Parameters"] = value
        else:
            print("Couldn't find key %s in template" % key)

    # Save updated template to the destination file
    with open(infraPath + fileToSave + ".template", 'w') as outfile:
        json.dump(data, outfile, indent=4, sort_keys=True)

# Uses a glueDBgeneral.template and injects function config into it.
def generateTableTemplate(fileToCopy, fileToSave, databaseName, config):

    # Creating python destination directory and .template file
    global infraPath
    global tempDir

    # Open the glueTablegeneral.template generic file
    with open(fileToCopy) as json_file:
        data = json.load(json_file)

    # Load the downloaded function function.template
    with open(tempDir + fileToSave) as json_file:
        databaseConfig = json.load(json_file)

    # Open the downloaded function.template
    # Inject the parameters, if keys exist update the template
    for key, value in databaseConfig["GLUE"].items():
        if key == "Name":
            data["Parameters"]["Name"]["Default"] = value
        elif key == "Description":
            data["Parameters"]["Description"]["Default"] = value
        elif key == "TableType":
            data["Parameters"]["TableType"]["Default"] = value
        elif key == "DatabaseName":
            data["Parameters"]["GlueDatabase"]["Default"] = value
        elif key == "PartitionKeys":
            data["Resources"]["GlueTable"]["Properties"]["TableInput"]["PartitionKeys"] = value
        elif key == "StorageDescriptor":
            data["Resources"]["GlueTable"]["Properties"]["TableInput"]["StorageDescriptor"] = value
        elif key == "Parameters":
            data["Resources"]["GlueTable"]["Properties"]["TableInput"]["Parameters"] = value


        else:
            print("Couldn't find key %s in template" % key)

    # Save updated template to the destination file
    with open(infraPath + fileToSave + ".template", 'w') as outfile:
        json.dump(data, outfile, indent=4, sort_keys=True)



def readConfig():

    # Creating python destination directories
    global infraPath
    global tempDir
    createFolder(infraPath)
    createFolder(tempDir)

    # Take workspace as parameter or set current directory
    if len(sys.argv) >= 2:
        path = sys.argv[1]
    else:
        path = "."
    print("Using this properties file: %s\n" % path)

    # Read JSON configurations file
    with open(path + "/glueProperties.json") as f:
        config = json.load(f)

    DBSourceTemplate = config["DEFAULT"]["targetDB"]
    DBtoGenerate = config["DEFAULT"]["sourceDB"]
    tablesToGenerate = config["DEFAULT"]["sourceTables"]
    tablesSourceTemplate = config["DEFAULT"]["targetTables"]
    return config, DBSourceTemplate, DBtoGenerate, tablesSourceTemplate, tablesToGenerate

def deployDatabases(SourceTemplate, toGenerate, config):
    for entry in toGenerate:
        generateDatabaseTemplate(SourceTemplate, entry + "_SourceDatabase", config)


def deployTables(client, SourceTemplate, toGenerate, databaseName, config):
    global tempDir
    # If we generate all of the tables scan existing templates to generate in workspace
    if len(toGenerate) < 1:
        response = client.get_tables(DatabaseName=databaseName)
        resCode = response["ResponseMetadata"]["HTTPStatusCode"]
        if resCode != 200:
            print("Request was not successful: %d" % resCode)
        else:
            for table in response["TableList"]:
                entry = table["Name"]
                generateTableTemplate(SourceTemplate, entry + "_SourceTable", databaseName, config)
    for entry in toGenerate:
        generateTableTemplate(SourceTemplate, entry + "_SourceTable", databaseName, config)

    createFolder(tempDir) # Clean Temp directory


# Removes a list of keys from the downloaded JSON
def cleanFunctionConfig(data):
    listOfRemovalKeys = [ "CreateTime", "CreateTableDefaultPermissions", "UpdateTime", "LastAccessTime", "CreatedBy",
                          "Owner", "IsRegisteredWithLakeFormation", "Retention"]
    for key in listOfRemovalKeys:
        if key in data["GLUE"]:
            data["GLUE"].pop(key, None) # If key exist remove it and return it's value
    return data

# Gets an array of function names and request the function deployment package and configuration
def parseDatabases(client, databases, toGenerate, tablesToGenerate):
    global tempDir

    # Generate Glue JSON, set into Temp folder
    for database in databases:
        databaseName = database["Name"]
        if any(databaseName in s for s in toGenerate):
            print("Working on database: %s\n" % databaseName)

            # Save database original parameters into the databaseName in Temp location
            with open( tempDir + databaseName + '_SourceDatabase', 'w' ) as outfile:
                print("Writing: %s to a JSON file in Inputs\n" % databaseName)
                data = {'GLUE': database}
                data = cleanFunctionConfig(data)
                print (json.dumps(data, indent=4, sort_keys=True) + "\n")
                json.dump(data, outfile, indent=4, sort_keys=True)

            getTables(client, databaseName, toGenerate, tablesToGenerate)


# Gets a list of resources from environment, it's required to update keys
def getTables(client, databaseName, toGenerate, tablesToGenerate):
    global tempDir
    data = []
    numOfTables = len(tablesToGenerate)
    if numOfTables > 0: # Generate each of the given tables
        for tableName in tablesToGenerate:
            response = client.get_table(DatabaseName=databaseName, Name=tableName)
            resCode = response["ResponseMetadata"]["HTTPStatusCode"]
            if resCode!= 200:
                print("Request was not successful: %d" % resCode )

            # If the req was successful generate a table template
            else:

                # Save Table parameters into the tableName in Temp location
                with open(tempDir + tableName + '_SourceTable', 'w') as outfile:
                    print("Writing: %s to a JSON file in Inputs\n" % databaseName)
                    data = {'GLUE': response["Table"]}
                    data = cleanFunctionConfig(data)
                    print(json.dumps(data, indent=4, sort_keys=True) + "\n")
                    json.dump(data, outfile, indent=4, sort_keys=True)

    else: # Generate all of the tables
        response = client.get_tables(DatabaseName=databaseName)
        resCode = response["ResponseMetadata"]["HTTPStatusCode"]
        if resCode != 200:
            print("Request was not successful: %d" % resCode)
        else:
            for table in response["TableList"]:
                tableName = table["Name"]
                # Save Table parameters into the tableName in Temp location
                with open(tempDir + tableName + 'SourceTable.template', 'w') as outfile:
                    print("Writing: %s to a JSON file in Inputs\n" % databaseName)
                    data = {'GLUE': table}
                    data = cleanFunctionConfig(data)
                    print(json.dumps(data, indent=4, sort_keys=True) + "\n")
                    json.dump(data, outfile, indent=4, sort_keys=True)
        print(response)


# Gets a list of resources from environment, it's required to update keys
def getDatabases(client, toGenerate, tablesToGenerate):
    data = []
    response = client.get_databases()
    data.extend(response["DatabaseList"])

    parseDatabases(client, data, toGenerate, tablesToGenerate)

def generateSchemaDict():
    # Open the keys file and parse line by line, dump into json output file
    data = []
    counter = 0
    with open("columns", "r") as file:
        for line in file:
            keyPair = line.replace(',', '').replace('`', '').strip().split(' ')
            entry = collections.OrderedDict()
            entry["Type"] = keyPair[1]
            entry["Name"] = keyPair[0]
            data.append(entry)
    print("Found %d new entries\n" % len(data))

    # Save resulted JSON to a file
    with open("columns-output", 'w') as outfile:
        # print json.dumps(data, indent=4, sort_keys=True) + "\n"
        json.dump(data, outfile, indent=4, sort_keys=True)

def main():
    # Before running, check properties.ini, Insert wanted function names as list
    config, DBSourceTemplate, DBtoGenerate, tablesSourceTemplate, tablesToGenerate = readConfig()

    # Make sure you have valid credentials, also make sure they are temporary and restricted in time
    # You can use Security Token Service to get a temporary token, it follows the best practice
    session = boto3.Session()
    region = session.region_name
    print("Currently using %s as the region" % region)
    if(region is None):
        print("ERROR: Make sure to set the region")
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()

    client = boto3.client('glue',
                          aws_access_key_id=current_credentials.access_key,
                          aws_secret_access_key=current_credentials.secret_key,
                          aws_session_token=current_credentials.token
                          )

    # Step 1, get the database config and files
    getDatabases(client, DBtoGenerate, tablesToGenerate)


    # Step 2, set the deployment folders and inject parameters to general.template
    deployDatabases(DBSourceTemplate, DBtoGenerate, config)
    deployTables(client, tablesSourceTemplate, tablesToGenerate, DBtoGenerate[0], config)

# Entry point
main()
# generateSchemaDict()