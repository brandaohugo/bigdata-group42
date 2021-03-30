import paramiko
import timeit
import csv

def execute_ssh_command(host, user, password, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,username=user, password=password)
    start_time = timeit.default_timer()
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    output = ssh_stdout.readlines()
    time_elapsed = timeit.default_timer() - start_time
    return output, time_elapsed


def append_to_csv(out_file, data):
    with open(out_file, "a+") as f:
            wr = csv.writer(f)
            wr.writerow(data)