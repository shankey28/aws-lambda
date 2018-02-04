The lambda function is called by s3 bucket putobject event
The database username/password and server address should be encrypted using the AWS KMS service
This example assumes that encryption and decryption of sensitive information will be taken care of in the code

Quick Code summary:
The code maps content of an object with json path /MySQL column name mapping data stored in a meta data file on s3.

The s3 object may have an array of rows which are processed and populated into a MySQL table

