# coding:utf-8
from cassandra import ConsistencyLevel
# 引入数据
from cassandra.cluster import Cluster
# 引入DCAwareRoundRobinPolicy模块，可用来自定义驱动程序的行为
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import commands, os, sys
import argparse

def cassandra_command_ops(cass_host_list, cassandra_command):
    for cass_host in cass_host_list:
        cass_host = cass_host.split(':')[0]
        check_nodetool = "ls " + filename
        while True:
            os.environ['cassandra_command'] = str(cassandra_command)
            os.environ['check_nodetool'] = str(check_nodetool)
            status, remote_repair = commands.getstatusoutput("ssh root@%s $check_nodetool && $cassandra_command" %(cass_host))
            print(status, remote_repair)
            if status == 0:
                break
            else:
                print(remote_repair)

def cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql):
    for host in cass_host_list:
        ip = host.split(':')[0]
        port = host.split(':')[1]
        cluster = Cluster(contact_points = [ip], port = int(port))
        session = cluster.connect()
        session.execute(graphindex_sql)
        session.execute(edgestore_sql)
        session.shutdown()
        cluster.shutdown()

def main():
    print(filename)
    try:
        cassandra_command = filename + " " + "repair" + " " + keyspace
        cassandra_command_ops(cass_host_list, cassandra_command)

        graphindex_sql = "ALTER TABLE" + " " + keyspace + ".graphindex WITH gc_grace_seconds = '0';"
        edgestore_sql = "ALTER TABLE" + " " + keyspace + ".edgestore WITH gc_grace_seconds = '0';"
        cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql)

        cassandra_command = filename + " " + "compact" + " " + keyspace
        cassandra_command_ops(cass_host_list, cassandra_command)

        graphindex_sql = "ALTER TABLE" + " " + keyspace + ".graphindex WITH gc_grace_seconds = '86400';"
        edgestore_sql = "ALTER TABLE" + " " + keyspace + ".edgestore WITH gc_grace_seconds = '86400';"
        cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql)

    except Exception, e:
        print(e)
        sys.exit(2)


if __name__ == "__main__":
    filename = '/opt/apache-cassandra-3.11.4/bin/nodetool'
#    if not os.path.exists(filename):
#        print("nodetool not exist!!")
#        sys.exit(2)

    parser = argparse.ArgumentParser(description='manual to this sceipt')
    parser.add_argument("--keyspace", type=str, required=True)
    parser.add_argument("--hosts", type=str, required=True)
    args = parser.parse_args()
    print(args.keyspace)
    print(args.hosts)
    
    keyspace = args.keyspace
    cass_host_list = args.hosts.split(',')
    main()

