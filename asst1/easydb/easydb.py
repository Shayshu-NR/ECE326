#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#
import socket
import math
import datetime
from struct import *
from .packet import *
from .exception import *


def nameTest(name, existingNames):
    # test table name for type error
    if not isinstance(name, str):
        raise TypeError("Name is not of type str!")
    # check the first character, must be a letter
    if not name[0].isalpha():
        raise ValueError("Name must start with a letter!")
    # check remaining characters, must be alphanumeric or underscore
    for char in name:
        if not char.isalnum() and not char == '_':
            raise ValueError(
                "Name must only contain letters, numbers, and underscores!")
    # check for duplicate names
    for otherName in existingNames:
        if name == otherName:
            raise ValueError("Duplicate name!")


class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.table_names = dict()
        table_id = 1

        # loop through each table in schema
        for table in tables:
            tableName = table[0]
            nameTest(tableName, self.table_names)

            # loop through each column in table
            columns = table[1]
            columnNames = list()
            for column in columns:
                columnName = column[0]
                # do checks on the column name
                nameTest(columnName, columnNames)
                if columnName == 'id':
                    raise ValueError("Column name is reserved keyword 'id'!")
                columnNames.append(columnName)

                # now do checks on the value
                columnValue = column[1]

                # check if foreign key
                if isinstance(columnValue, str) or columnValue is None:
                    if columnValue is None:
                        break
                    # check for backwards declaration
                    error = True
                    # if
                    if columnValue in self.table_names:
                        error = False
                        break
                    if error:
                        raise IntegrityError(
                            "Foreign Key causes cycle or references nonexistant table!")

                # if not a foreign key, check if it is one of the allowed types (int float str)
                elif not columnValue == str and not columnValue == int and not columnValue == float:
                    raise ValueError("type is not str, int or float!")
            # now that table has been properly checked, add to the list as a defined table
            self.table_names[tableName] = ([table_id, table[1]])
            table_id = table_id + 1

        self.schema = tables

    def connect(self, host, port):
        # address is a tuple used to create the connection to the server
        address = (host, port)

        # connect to the address
        self.connection.connect(address)

        # wait for the response code
        while True:
            data = self.connection.recv(4096)
            error_code = unpack('!i', data)

            if error_code[0] == SERVER_BUSY:
                # close the connection
                exit_command = pack('!i', EXIT)
                self.connection.sendall(exit_command)
                self.connection.close()
                return False

            elif error_code[0] == OK:
                # otherwise we're all good!
                return True

            else:
                # uh something went very wrong...
                return False

    def close(self):
        # first send the exit command
        exit_command = pack('!i', EXIT)
        self.connection.sendall(exit_command)
        self.connection.close()
        pass

    def insert(self, table_name, values):
        # start by doing some error checking to make sure all the values and types are correct

        if table_name in self.table_names:
            # the correct table was selected!
            # make sure the number of values supplied was sufficient

            tableid = self.table_names[table_name][0]

            if len(values) == len(self.table_names[table_name][1]):
                # now check to make sure that the values are correct
                i = 0

                for entries in self.table_names[table_name][1]:
                    # check if the types match
                    if type(values[i]) != entries[1]:
                        # check if it's a foreign key...
                        if type(values[i]) != int and entries[1] in self.table_names:
                            raise InvalidReference(
                                "Trying to pass non-int for foreign key")
                        elif type(values[i]) != entries[1] and entries[1] not in self.table_names:

                            raise PacketError("Wrong type supplied")
                    i = i + 1
            else:
                raise PacketError("Number of values supplied (" +
                                  str(len(values)) + ") doesn't match the number of rows")
        else:
            raise PacketError(
                "Table name: " + str(table_name) + " does not exist!")

        # now construct the request
        request = pack('!ii', INSERT, tableid)
        row = pack('!i',  len(self.schema[tableid - 1][1]))
        to_be_inserted = []
        i = 0

        for entries in values:
            if type(entries) == int:
                # check if it's a foreign key....
                if self.table_names[table_name][1][i][1] in self.table_names:
                    entry_type = FOREIGN
                else:
                    entry_type = INTEGER

                int_entry = pack('!iiq', entry_type, 8, entries)
                to_be_inserted.append(int_entry)

            elif type(entries) == float:
                float_entry = pack('!iid', FLOAT, 8, entries)
                to_be_inserted.append(float_entry)

            elif type(entries) == str:
                size_of_string = 4 * math.ceil(len(entries)/4.0)
                str_entry = b''.join([pack('!ii', STRING, size_of_string), entries.encode(
                    'ascii'), b'\x00' * (size_of_string - len(entries))])
                to_be_inserted.append(str_entry)

            else:
                raise Exception("Invalid typing")
            i = i + 1

        insert_statement = request + row + b''.join(to_be_inserted)

        # now send the packet...
        self.connection.sendall(insert_statement)

        while True:
            data = self.connection.recv(4096)
            if len(data) == 4:
                error_code = unpack('!i', data)
            elif len(data) == 20:
                error_code = unpack('!iqq', data)

            if error_code[0] == OK:
                return (error_code[1], error_code[2])
            else:
                if error_code[0] == BAD_FOREIGN:
                    raise InvalidReference(
                        "The give foreign key could not be referenced")
                else:
                    raise PacketError("Failed insert")

        return

    def update(self, table_name, pk, values, version=None):
        # first perform error checks on input
        if type(pk) != int:
            raise PacketError("pk is not an int!")

        if version != None:
            if type(version) != int:
                raise PacketError("version is not an int!")

        if table_name in self.table_names:
            tableid = self.table_names[table_name][0]

            # check number of values given to number of values in row
            if len(values) == len(self.table_names[table_name][1]):
                # now check that each value matches its column
                for i, entries in enumerate(self.table_names[table_name][1]):
                    # check for mismatch
                    if type(values[i]) != entries[1]:
                        # mismatch may still valid if foreign key
                        # check if not a foreign key
                        if type(values[i]) != int and entries[1] in self.table_names:
                            raise InvalidReference(
                                "trying to pass non-int for foreign key")

                        elif type(values[i]) != entries[1] and entries[1] not in self.table_names:
                            raise PacketError(
                                "supplied type does not match row")

            else:
                raise PacketError(
                    "number of values given does not match number of columns")
        else:
            raise PacketError("Table name does not exist!")

        # error checks passed, construct request
        request = pack('!ii', UPDATE, tableid)
        if version == None:
            key = pack('!qq', pk, 0)
        else:
            key = pack('!qq', pk, version)
        row = pack('!i', len(self.schema[tableid-1][1]))
        to_be_updated = []

        # pack the values
        for i, value in enumerate(values):
            # print(value)
            if type(value) == int:
                if self.table_names[table_name][1][i][1] in self.table_names:
                    entry_type = FOREIGN
                else:
                    entry_type = INTEGER
                int_entry = pack("!iiq", entry_type, 8, value)
                to_be_updated.append(int_entry)

            elif type(value) == float:
                float_entry = pack('!iid', FLOAT, 8, value)
                to_be_updated.append(float_entry)

            elif type(value) == str:
                string_length = 4 * math.ceil(len(value)/4.0)
                string_entry = b''.join([pack('!ii', STRING, string_length), value.encode(
                    'ascii'), b'\x00'*(string_length - len(value))])
                to_be_updated.append(string_entry)

            else:
                raise Exception("Invalid Typing")

        update_statement = request + key + row + b''.join(to_be_updated)
        # send the packet
        # print(update_statement)
        self.connection.sendall(update_statement)

        # receive and handle response
        while True:
            data = self.connection.recv(4096)
            # print(data)
            if len(data) == 4:
                error_code = unpack('!i', data)
            elif len(data) == 12:
                error_code = unpack('!iq', data)

            print(error_code)
            if error_code[0] == OK:
                return (error_code[1])
            elif error_code[0] == NOT_FOUND:
                raise ObjectDoesNotExist("Row not found!")
            elif error_code[0] == TXN_ABORT:
                raise TransactionAbort("Atomic update failed!")
            elif error_code[0] == BAD_FOREIGN:
                raise InvalidReference(
                    "Foreign field reference does not exist!")
            else:
                raise PacketError("Update failed!")

    def drop(self, table_name, pk):
        # check to see if the table actually exists
        if table_name not in self.table_names:
            raise PacketError("Table " + str(table_name) + " does not exist")

        if type(pk) != int:
            raise PacketError("Invalid row type given!")

        # compose the query then simply parse the error code
        drop_statement = pack(
            '!iiq', DROP, self.table_names[table_name][0], pk)

        self.connection.sendall(drop_statement)

        while True:
            data = self.connection.recv(4096)
            error_code = unpack('!i', data)

            if error_code[0] == NOT_FOUND:
                raise ObjectDoesNotExist(
                    "Could not find row with id " + str(pk))
            elif error_code[0] == OK:
                return
            else:
                raise PacketError("Failed to drop table")
        pass

    def get(self, table_name, pk):
        # make sure the table name is in the db...
        if table_name not in self.table_names:
            raise PacketError("That table doesn't exist!")

        # make sure that pk is a primary key
        if type(pk) != int:
            raise PacketError("That isn't a valid primary key!")

        # create the get statement
        get_statement = pack('!iiq', GET, self.table_names[table_name][0], pk)

        self.connection.sendall(get_statement)

        while True:
            data = self.connection.recv(4096)

            # check if the get was successful
            if len(data) == 4:
                error_code = unpack('!i', data)
                if error_code[0] == NOT_FOUND:
                    raise ObjectDoesNotExist(
                        "Couldn't find row with given primary key")

            # otherwise start parsing the data
            else:
                index = 16
                row_info = []
                version = unpack('!q', data[4:12])

                while index in range(16, len(data[16:]) + 1):
                    value_type = unpack('!i', data[index: index + 4])
                    size = unpack('!i', data[index + 4: index + 8])

                    # now check the type...
                    if value_type[0] == INTEGER or value_type[0] == FOREIGN:
                        unpacked_int = unpack(
                            '!q', data[index + 8: index + 8 + size[0]])
                        row_info.append(unpacked_int[0])

                    elif value_type[0] == FLOAT:
                        unpacked_float = unpack(
                            '!d', data[index + 8: index + 8 + size[0]])
                        row_info.append(unpacked_float[0])

                    elif value_type[0] == STRING:
                        unpacked_string = data[index +
                                               8: index + 8 + size[0]].decode('ascii')
                        unpacked_string = ''.join(unpacked_string)
                        unpacked_string = unpacked_string.strip('\x00')
                        row_info.append(unpacked_string)

                    else:
                        raise PacketError(
                            "Oh no that type doesn't even exist!")

                    index = index + 8 + size[0]

                return (row_info, version[0])

        pass

    def scan(self, table_name, op, column_name=None, value=None):
        # first check that the table name is valid
        if table_name not in self.table_names:
            raise PacketError(
                "Table name: " + str(table_name) + " does not exist")

        # check to make sure the operation is valid
        if type(op) != int or op not in range(1, 8):
            raise PacketError("Invalid operation")

        # make sure if you're given an op that isn't AL that column and value are not none
        if op in range(2, 8) and (column_name is None or value is None):
            raise PacketError("Missing value and/or column_name")
        elif type(column_name) != str and op != operator.AL:
            raise PacketError("A non string column name was given!")

        # check if the column_name is valid
        if op != operator.AL and column_name != "id":
            col_id = 1
            col_found = False
            for column in self.table_names[table_name][1]:
                if column_name in column[0]:
                    # check if it's a foreign key

                    if type(column[1]) == str and column[1] in self.table_names:
                        # check if the value is an int...
                        if type(value) != int:
                            raise PacketError("Invalid value passed: ", value)

                        # check that the operation is valid...
                        if op != operator.EQ and op != operator.NE:
                            raise PacketError(
                                "Invalid operation for foreign key", op)

                        col_found = True
                        break
                    elif type(value) != column[1]:
                        raise PacketError(
                            "Invalid value passed: ", column[0], value, type(value))
                    else:
                        col_found = True
                        break
                col_id = col_id + 1

            if not col_found:
                raise PacketError("No column with name: " +
                                  str(column_name) + " found")
        else:
            col_id = 0

        # now that everything is error checked, build the scan statement...
        if op == operator.AL:
            request = pack('!ii', SCAN, self.table_names[table_name][0])
            col_op_values = pack('!iiii', 0, op, NULL, 0)

            scan_statement = b''.join([request, col_op_values])
        else:
            request = pack('!ii', SCAN, self.table_names[table_name][0])
            col_op = pack('!ii', col_id, op)

            if type(value) == int:
                # check if it's a foreign key....
                if self.table_names[table_name][1][col_id - 1][1] in self.table_names or column_name == "id":
                    entry_type = FOREIGN
                else:
                    entry_type = INTEGER

                int_entry = pack('!iiq', entry_type, 8, value)

                to_be_inserted = int_entry

            elif type(value) == float:
                float_entry = pack('!iid', FLOAT, 8, value)

                to_be_inserted = float_entry

            elif type(value) == str:
                size_of_string = 4 * math.ceil(len(value)/4.0)

                str_entry = b''.join([pack('!ii', STRING, size_of_string), value.encode(
                    'ascii'), b'\x00' * (size_of_string - len(value))])

                to_be_inserted = str_entry

            else:
                raise Exception("Invalid typing")

            scan_statement = b''.join([request, col_op, to_be_inserted])

        self.connection.sendall(scan_statement)

        while True:
            data = self.connection.recv(4096)
            if len(data) > 4:
                id_size = len(data) - 8
                id_count = "q" * int((id_size/8))
                unpack_format = "!ii" + id_count
            else:
                unpack_format = "!i"

            unpacked_data = unpack(unpack_format, data)

            if unpacked_data[0] == OK:
                ids_to_return = list()

                for i in range(2, unpacked_data[1] + 2):
                    ids_to_return.append(unpacked_data[i])

                return ids_to_return
            else:
                raise PacketError("Failed to scan for rows")
