import csv
import socket
import time

host = "localhost"
port = 9999
address_tuple = (host, port)

socket_family = socket.AF_INET 

socket_type = socket.SOCK_DGRAM 

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

# read from file
input_file = open("sales_data_sample.csv", "r")

reversed = sorted(input_file)

# create a csv reader
reader = csv.reader(reversed, delimiter=",")

# create writable file for output
output_file = open("out9.txt", "w")

for row in reader:
    ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,ORDERDATE,STATUS,QTR_ID,MONTH_ID,YEAR_ID,PRODUCTLINE,MSRP,PRODUCTCODE,CUSTOMERNAME,PHONE,ADDRESSLINE1,ADDRESSLINE2,CITY,STATE,POSTALCODE,COUNTRY,TERRITORY,CONTACTLASTNAME,CONTACTFIRSTNAME,DEALSIZE = row

    # use an fstring to create a message from our data
    fstring_message = f"[{ORDERNUMBER},{QUANTITYORDERED},{PRICEEACH},{ORDERLINENUMBER},{SALES},{ORDERDATE},{STATUS},{QTR_ID},{MONTH_ID},{YEAR_ID},{PRODUCTLINE},{MSRP},{PRODUCTCODE},{CUSTOMERNAME},{PHONE},{ADDRESSLINE1},{ADDRESSLINE2},{CITY},{STATE},{POSTALCODE},{COUNTRY},{TERRITORY},{CONTACTLASTNAME},{CONTACTFIRSTNAME},{DEALSIZE}]"
    
    # prepare a binary (1s and 0s) message to stream
    MESSAGE = fstring_message.encode()

    # send the message
    sock.sendto(MESSAGE, address_tuple)
    print (f"Sent: {MESSAGE} on port {port}.")

    time.sleep(1)

    # write to new file
    output_file.write(fstring_message)
