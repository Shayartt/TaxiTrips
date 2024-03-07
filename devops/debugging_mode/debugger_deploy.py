import paramiko
import os
from sftp_subclass import MySFTPClient
import traceback


# Define local folder path and SSH server details
local_folder_path = "YOUR_CODE_PATH"
ssh_host = "ANY_INSTANCE_WITH_EFS_IP"
ssh_port = 22 
ssh_username = "hadoop"
private_key_path = "SSH_KEY_PATH"
remote_folder_path = f"/efs/traffic/code" # Feel free to change it as you want.


#Check file permissions : 
if os.access(local_folder_path, os.R_OK):
    print("Read access granted")
else:
    print("Read access denied")
    
# Create an SSH client
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Load the private key for authentication
private_key = paramiko.RSAKey(filename=private_key_path)

try:
    # Connect to the SSH server with private key authentication
    transport = paramiko.Transport((ssh_host, ssh_port))
    transport.connect(username=ssh_username, pkey=private_key)
    # ssh.connect(ssh_host, port=ssh_port, username=ssh_username, pkey=private_key)

    # Create an SFTP client
    sftp = MySFTPClient.from_transport(transport)


    # Upload the local folder to the remote server
    sftp.mkdir(remote_folder_path, ignore_existing=True)
    sftp.put_dir(local_folder_path, remote_folder_path)
    sftp.close()
    # sftp.put(local_folder_path, remote_folder_path, confirm=True)

    print("Folder copied successfully")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print(traceback.format_exc())
finally:
    # Close the SFTP and SSH connections
    sftp.close()
    ssh.close()
